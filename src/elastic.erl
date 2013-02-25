-module(elastic).

% General RepObj interface
-export([
      new/4,
      new_replica/3,
      cast/2,
      call/3,
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
      makeImmutable/5
   ]).

% Server callbacks
-export([
      init/2,
      handle_msg/3
   ]).

-include("libdist.hrl").

% Create a new elastically replicated state machine
new(CoreSettings = {CoreModule, _}, ElasticArgs, Nodes, Retry) ->
   % spawn new replicas
   Replicas = [ new_replica(N , CoreSettings, ElasticArgs) || N <- Nodes ],

   Conf = set_conf_args([{core_module, CoreModule} | ElasticArgs], #rconf{
         protocol = ?MODULE,
         version = 1,
         pids = Replicas
      }),

   % activate and return the new configuration
   libdist_utils:multicall(Replicas, {activate, Conf}, Retry),
   Conf.


% Start a new replica
new_replica(Node, CoreSettings, _ElasticArgs) ->
   server:start(Node, ?MODULE, CoreSettings).


cast(_Conf, _Command) ->
   {error, not_valid_for_this_protocol}.

% Send a command to a replicated object
call(#rconf{pids=[Orderer | _], version=Vn, args=Args}, Command, Retry) ->
   CoreModule = proplists:get_value(cmod, Args),
   CommandType = case CoreModule:is_mutating(Command) of
      false -> read;
      true -> write
   end,
   libdist_utils:call(Orderer, CommandType, {Vn, Command}, Retry).


% Reconfigure the replicated object with a new set of replicas
reconfigure(OldConf, NewReplicas, NewArgs, Retry) ->
   #rconf{version = Vn, pids = OldReplicas, args = OldArgs} = OldConf,
   NewConfArgs = [{core_module, proplists:get_value(cmod, OldArgs)} | NewArgs],
   NewConf = set_conf_args(NewConfArgs, OldConf#rconf{version=Vn+1, pids=NewReplicas}),

   % wedge the old configuration by wedging any of its replicas
   libdist_utils:anycall(OldReplicas, {wedge, Vn}, Retry),

   % any active replicas in NewReplicas should be wedged (this could happen if
   % NewReplicas's intersection with OldReplicas is non-empty)
   libdist_utils:multicall(NewReplicas, {wedge, Vn}, Retry),

   % Update the NewReplicas to use the new configuration:
   % replicas in a PENDING state should inherit the state of a replica in the
   % prior configuration, while replicas from the prior configuration in a
   % WEDGED state can just update their configuration
   {Pending, Wedged} = lists:foldl(
      fun(Pid, {Pending, Wedged}) ->
            case libdist_utils:call(Pid, ping, Retry) of
               pending ->
                  {[Pid | Pending], Wedged};
               {wedged, #rconf{version = Vn}} ->
                  {Pending, [Pid | Wedged]}
            end
      end,
      {[], []},
      NewReplicas),

   % since PENDING replicas can only inherit from wedged replicas in the old
   % configuration, do that first before unwedging wedged replicas
   libdist_utils:multicall(Pending, {inherit, OldConf, NewConf, Retry}, Retry),
   libdist_utils:multicall(Wedged, {update_conf, Vn, NewConf}, Retry),
   NewConf.    % return the new configuration


% Stop one of the replicas of the replicated object.
stop(Obj=#rconf{version = Vn, pids = OldReplicas}, N, Reason, Retry) ->
   libdist_utils:multicall(OldReplicas, {wedge, Vn}, Retry),    % wedge all replicas
   Pid = lists:nth(N, OldReplicas),
   libdist_utils:call(Pid, {stop, Reason}, Retry),
   NewReplicas = lists:delete(Pid, OldReplicas),
   NewConf = Obj#rconf{version = Vn + 1, pids = NewReplicas},
   libdist_utils:multicall(NewReplicas, {update_conf, Vn, NewConf}, Retry),
   NewConf.


% Wedge a configuration
wedge(#rconf{version = Vn, pids = Pids}, Retry) ->
   libdist_utils:anycall(Pids, {wedge, Vn}, Retry).

% Wedge a particular replica in a configuration
wedge(#rconf{version = Vn, pids = Pids}, N, Retry) ->
   Pid = lists:nth(N, Pids),
   libdist_utils:call(Pid, {wedge, Vn}, Retry).



%%%%%%%%%%%%%%%%%%%%%%
% Callback Functions %
%%%%%%%%%%%%%%%%%%%%%%

% Initialize a new replica
init(_Me, {CoreModule, CoreArgs}) ->
   {pending, CoreModule, CoreArgs}.


makeImmutable(Core, Conf, Unstable, StableCount, NextCmdNum) ->
   {immutable, Core, Conf, Unstable, StableCount, NextCmdNum}.


% Handle a queued message

% A replica is in a pending state while it awaits transition to activation
% with a state that is either new (activate) or inherited from a prior
% configuration (inherit)
handle_msg(Me, Message, {pending, CoreModule, CoreArgs}) ->
   case Message of
      % activate a configuration for a 'fresh' replicated object
      {Ref, Client, {activate, Conf=#rconf{version=1, pids=Pids, args=ConfArgs}}} ->
         case lists:member(Me, Pids) of
            true ->
               Core = libdist_sm:new(CoreModule, CoreArgs),
               Unstable = ets:new(unstable_commands, []),
               Client ! {Ref, ok},
               ElasticModule = proplists:get_value(emod, ConfArgs),
               server:prepend_handler(Me, ElasticModule, []),
               {consume, ElasticModule:activate(Me, Core, Conf, Unstable, 0, 0)};
            false ->
               consume
         end;

      % activate a configuration and inherit the state from a prior config
      {Ref, Client, {inherit, {_OldConf=#rconf{version = Vn, pids = OldPids},
                               NewConf=#rconf{version = Vn2, pids = NewPids, args=NewConfArgs},
                               Retry}} } when Vn2 == Vn+1 ->
         case lists:member(Me, NewPids) of
            true ->
               % TODO: set new replica to get the full state from old replica
               Core = libdist_sm:new(CoreModule, CoreArgs),
               [{_Responder, {UnstableList, StableCount, NextCmdNum}}] =
                  libdist_utils:anycall(OldPids, {inherit, Vn, node(), CoreArgs}, Retry),
               Unstable = ets:new(unstable_commands, []),
               ets:insert(Unstable, UnstableList),
               Client ! {Ref, ok},
               ElasticModule = proplists:get_value(emod, NewConfArgs),
               server:remove_handler(Me, elastic_chain),
               server:remove_handler(Me, elastic_primary_backup),
               server:prepend_handler(Me, ElasticModule, []),
               {consume, ElasticModule:activate(Me, Core, NewConf, Unstable,
                     StableCount, NextCmdNum)};

            false ->
               consume
         end;

      % respond to all other queries with a 'pending' message
      {Ref, Client, _} ->
         Client ! {Ref, {error, pending}},
         consume
   end;


% State: IMMUTABLE
% An immutable replica remains so for the rest of its life in this configuration.
% The only things it can do is pass down its history (stable + unstable) to a
% caller, or upgrade to a successor configuration.
handle_msg(Me, Message, {immutable,
      Core, Conf=#rconf{version=Vn}, Unstable, StableCount, NextCmdNum}) ->
   case Message of
      % For practical considerations, a replica can inherit its history from a
      % prior configuration (i.e. upgrade its configuration).
      {Ref, Client, {update_conf, Vn, NewConf=#rconf{
               version = Vn2, pids = Pids, args = NewConfArgs}} } when Vn2 == Vn+1->
         case lists:member(Me, Pids) of
            true ->
               Client ! {Ref, ok},
               ElasticModule = proplists:get_value(emod, NewConfArgs),
               {consume, ElasticModule:activate(Me, Core, NewConf, Unstable,
                     StableCount, NextCmdNum)};
            false ->
               consume
         end;

      % Request to inherit local state by a pending replica
      {Ref, Replica, {inherit, Vn}} ->
         % TODO: set inheriting replica to receive the current state of the core
         % (i.e. state transfer, perhaps via proto-replica mechanism)
         UnstableList = ets:tab2list(Unstable),
         Replica ! {Ref, {UnstableList, StableCount, NextCmdNum}},
         consume;

      % return the current configuration
      {Ref, Client, get_conf} ->
         Client ! {Ref, Conf},
         consume;

      % stop this replica
      {Ref, Client, {stop, Reason}} ->
         Client ! {Ref, Core:stop(Reason)},
         {stop, Reason};

      % for other tagged  messages, return a 'wedged' message
      {Ref, Client, _} ->
         Client ! {Ref, {error, {wedged, Conf}}},
         consume;

      % ignore everything else
      _ ->
         consume
   end;

handle_msg(_, _, _) ->
   no_match.


%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%


set_conf_args(Args, Conf) ->
   CoreModule = proplists:get_value(core_module, Args),
   ElasticModule = case proplists:get_bool(primary_backup, Args) of
      true -> elastic_primary_backup;
      false -> elastic_chain
   end,

   ConfArgs = [{cmod, CoreModule} , {emod, ElasticModule}],
   Conf#rconf{args = ConfArgs}.

