-module(elastic).

% General RepObj interface
-export([
      new/4,
      new_replica/2,
      do/3,
      fork/4,
      reconfigure/4,
      stop/4
   ]).

% Elastic replication added functions
-export([
      wedge/2,
      wedge/3
   ]).

% Elastic protocol callbacks
-export([
      immutableLoop/5,
      fork/7
   ]).

-include("repobj.hrl").

% Create a new elastically replicated state machine
new(CoreSettings = {CoreModule, _}, ElasticArgs, Nodes, Retry) ->
   % spawn new replicas
   Replicas = [
      spawn(N, ?MODULE, new_replica, [CoreSettings, ElasticArgs]) || N <- Nodes],

   Conf = set_conf_args([{core_module, CoreModule} | ElasticArgs], #conf{
         protocol = ?MODULE,
         version = 1,
         pids = Replicas
      }),

   % activate and return the new configuration
   repobj_utils:multicall(Replicas, activate, Conf, Retry),
   Conf.

% Start a new replica
new_replica({CoreModule, CoreArgs}, _ElasticArgs) ->
   pendingLoop(CoreModule, CoreArgs).

% Send a command to a replicated object
do(#conf{pids=[Orderer | _], version=Vn, args=Args}, Command, Retry) ->
   CoreModule = proplists:get_value(cmod, Args),
   CommandType = case CoreModule:is_mutating(Command) of
      false -> read;
      true -> write
   end,
   repobj_utils:call(Orderer, CommandType, {Vn, Command}, Retry).

% Fork one of the replicas in this replicated object
fork(_Obj=#conf{pids = Pids, version = Vn}, N, Node, Args) ->
   Pid = lists:nth(N, Pids),
   repobj_utils:cast(Pid, fork, {Vn, Node, Args}).

% Reconfigure the replicated object with a new set of replicas
reconfigure(OldConf, NewReplicas, NewArgs, Retry) ->
   #conf{version = Vn, pids = OldReplicas, args = OldArgs} = OldConf,
   NewConfArgs = [{core_module, proplists:get_value(cmod, OldArgs)} | NewArgs],
   NewConf = set_conf_args(NewConfArgs, OldConf#conf{version=Vn+1, pids=NewReplicas}),

   % wedge the old configuration by wedging any of its replicas
   repobj_utils:anycall(OldReplicas, wedge, Vn, Retry),

   % any active replicas in NewReplicas should be wedged (this could happen if
   % NewReplicas's intersection with OldReplicas is non-empty)
   repobj_utils:multicall(NewReplicas, wedge, Vn, Retry),

   % Update the NewReplicas to use the new configuration:
   % replicas in a PENDING state should inherit the state of a replica in the
   % prior configuration, while replicas from the prior configuration in a
   % WEDGED state can just update their configuration
   {Pending, Wedged} = lists:foldl(
      fun(Pid, {Pending, Wedged}) ->
            case repobj_utils:call(Pid, ping, ping, Retry) of
               pending ->
                  {[Pid | Pending], Wedged};
               {wedged, #conf{version = Vn}} ->
                  {Pending, [Pid | Wedged]}
            end
      end,
      {[], []},
      NewReplicas),

   % since PENDING replicas can only inherit from wedged replicas in the old
   % configuration, do that first before unwedging wedged replicas
   repobj_utils:multicall(Pending, inherit, {OldConf, NewConf, Retry}, Retry),
   repobj_utils:multicall(Wedged, update_conf, {Vn, NewConf}, Retry),
   NewConf.    % return the new configuration

% Stop one of the replicas of the replicated object.
stop(Obj=#conf{version = Vn, pids = OldReplicas}, N, Reason, Retry) ->
   repobj_utils:multicall(OldReplicas, wedge, Vn, Retry),    % wedge all replicas
   Pid = lists:nth(N, OldReplicas),
   repobj_utils:call(Pid, stop, Reason, Retry),
   NewReplicas = lists:delete(Pid, OldReplicas),
   NewConf = Obj#conf{version = Vn + 1, pids = NewReplicas},
   repobj_utils:multicall(NewReplicas, update_conf, {Vn, NewConf}, Retry),
   NewConf.


% Wedge a configuration
wedge(#conf{version = Vn, pids = Pids}, Retry) ->
   repobj_utils:anycall(Pids, wedge, Vn, Retry).

% Wedge a particular replica in a configuration
wedge(#conf{version = Vn, pids = Pids}, N, Retry) ->
   Pid = lists:nth(N, Pids),
   repobj_utils:call(Pid, wedge, Vn, Retry).


%%%%%%%%%%%%%%%%%%%%%%
% Protocol Callbacks %
%%%%%%%%%%%%%%%%%%%%%%


% State: IMMUTABLE
% An immutable replica remains so for the rest of its life in this configuration.
% The only things it can do is pass down its history (stable + unstable) to a
% caller, or upgrade to a successor configuration.
immutableLoop(Core, Conf=#conf{version=Vn}, Unstable, StableCount, NextCmdNum) ->
   receive
      % For practical considerations, a replica can inherit its history from a
      % prior configuration (i.e. upgrade its configuration).
      {Ref, Client, update_conf,
         {Vn, NewConf=#conf{version = Vn2, pids = Pids, args = NewConfArgs}}
      } when Vn2 == Vn+1->
         case lists:member(self(), Pids) of
            true ->
               Client ! {Ref, ok},
               ElasticModule = proplists:get_value(emod, NewConfArgs),
               ElasticModule:activeLoop(Core, NewConf, Unstable, StableCount, NextCmdNum);
            false ->
               immutableLoop(Core, Conf, Unstable, StableCount, NextCmdNum)
         end;

      % Request to inherit local state by a pending replica
      {Ref, Replica, inherit, {Vn, ForkNode, ForkArgs}} ->
         ForkedCore = Core:fork(ForkNode, ForkArgs),
         UnstableList = ets:tab2list(Unstable),
         Replica ! {Ref, {ForkedCore, UnstableList, StableCount, NextCmdNum}},
         immutableLoop(Core, Conf, Unstable, StableCount, NextCmdNum);

      % create a forked copy of this replica on this node
      {Ref, Client, fork, {Vn, ForkNode, ForkArgs}} ->
         Client ! {Ref, fork(Core, Conf, Unstable, StableCount, NextCmdNum,
               ForkNode, ForkArgs)},
         immutableLoop(Core, Conf, Unstable, StableCount, NextCmdNum);

      % return the current configuration
      {Ref, Client, get_conf} ->
         Client ! {Ref, Conf},
         immutableLoop(Core, Conf, Unstable, StableCount, NextCmdNum);

      % stop this replica
      {Ref, Client, stop, Reason} ->
         Client ! {Ref, Core:stop(Reason)};


      % for other tagged  messages, return a 'wedged' message
      {Ref, Client, _, _} ->
         Client ! {Ref, {error, {wedged, Conf}}},
         immutableLoop(Core, Conf, Unstable, StableCount, NextCmdNum);

      % ignore everything else
      _ ->
         immutableLoop(Core, Conf, Unstable, StableCount, NextCmdNum)
   end.

fork(Core, Conf, Unstable, StableCount, NextCmdNum, ForkNode, ForkArgs) ->
   ForkedCore = Core:fork(ForkNode, ForkArgs),
   UnstableList = ets:tab2list(Unstable),

   spawn(ForkNode, fun() ->
            ForkedUnstable = ets:new(unstable_commands, []),
            ets:insert(ForkedUnstable, UnstableList),
            immutableLoop(ForkedCore, Conf, ForkedUnstable, StableCount,
               NextCmdNum)
      end).



%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%


% A replica is in a pending state while it awaits transition to activation
% with a state that is either new (activate) or inherited from a prior
% configuration (inherit)
pendingLoop(CoreModule, CoreArgs) ->
   receive
      % activate a configuration for a 'fresh' replicated object
      {Ref, Client, activate, Conf=#conf{version=1, pids=Pids, args=ConfArgs}} ->
         case lists:member(self(), Pids) of
            true ->
               Core = sm:new(CoreModule, CoreArgs),
               Unstable = ets:new(unstable_commands, []),
               Client ! {Ref, ok},
               ElasticModule = proplists:get_value(emod, ConfArgs),
               ElasticModule:activeLoop(Core, Conf, Unstable, 0, 0);
            false ->
               pendingLoop(CoreModule, CoreArgs)
         end;

      % activate a configuration and inherit the state from a prior config
      {Ref, Client, inherit, {_OldConf=#conf{version = Vn, pids = OldPids},
                               NewConf=#conf{version = Vn2, pids = NewPids, args=NewConfArgs},
                               Retry} } when Vn2 == Vn+1 ->
         case lists:member(self(), NewPids) of
            true ->
               [{_Responder, {Core, UnstableList, StableCount, NextCmdNum}}] =
                  repobj_utils:anycall(OldPids, inherit, {Vn, node(), CoreArgs}, Retry),
               Unstable = ets:new(unstable_commands, []),
               ets:insert(Unstable, UnstableList),
               Client ! {Ref, ok},
               ElasticModule = proplists:get_value(emod, NewConfArgs),
               ElasticModule:activeLoop(Core, NewConf, Unstable, StableCount, NextCmdNum);

            false ->
               pendingLoop(CoreModule, CoreArgs)
         end;

      % respond to all other queries with a 'pending' message
      {Ref, Client, _, _} ->
         Client ! {Ref, {error, pending}},
         pendingLoop(CoreModule, CoreArgs)
   end.

set_conf_args(Args, Conf) ->
   CoreModule = proplists:get_value(core_module, Args),
   ElasticModule = case proplists:get_bool(primary_backup, Args) of
      true -> elastic_primary_backup;
      false -> elastic_chain
   end,

   ConfArgs = [{cmod, CoreModule} , {emod, ElasticModule}],
   Conf#conf{args = ConfArgs}.

