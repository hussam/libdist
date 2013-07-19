-module(elastic).
-behaviour(replica).

% replica callbacks
-export([
      type/0,
      overloads/1,
      conf_args/1,
      cast/2,
      init_replica/2,
      import/1,
      export/1,
      update_state/3,
      handle_failure/5,
      handle_msg/5
   ]).

% Elastic replication added functions
-export([
      new/4,
      reconfigure/3,
      wedge/2
   ]).


-include("constants.hrl").
-include("helper_macros.hrl").
-include("libdist.hrl").

-include("elastic.hrl").


% Create a new elastically replicated state machine
new(PrtclArgs, SMSettings = {SMModule, _}, Nodes, Timeout) ->
   % spawn new replicas
   Replicas = [ replica:new(?MODULE, PrtclArgs, SMSettings, N) || N <- Nodes ],
   Conf = #conf{
      type     = ?REPL,
      protocol = ?MODULE,
      version  = 1,
      sm_mod   = SMModule,
      replicas = Replicas
   },
   libdist_utils:multicall(Conf#conf.replicas, {1, {activate, Conf}}, Timeout),
   {ok, Conf}.


% Reconfigure the replicated object with a new set of replicas
reconfigure(_OldConf=#conf{version=0}, NewConf=#conf{replicas=Pids}, Timeout) ->
   NextConf = NewConf#conf{version = 1},
   libdist_utils:multicall(Pids, {1, {activate, NextConf}}, Timeout),
   {ok, NextConf};

reconfigure(OldConf, NewConf, Timeout) ->
   #conf{replicas = NewReplicas} = NewConf,
   #conf{version = OldVn, replicas = OldReplicas} = OldConf,
   NextConf = NewConf#conf{version = OldVn + 1},

   % wedge the old configuration by wedging any of its replicas
   libdist_utils:anycall(OldReplicas, {OldVn, wedge}, Timeout),

   % any active replicas in NewReplicas should be wedged (this could happen if
   % NewReplicas's intersection with OldReplicas is non-empty)
   libdist_utils:multicall(NewReplicas, {OldVn, wedge}, Timeout),

   % Update the NewReplicas to use the new configuration:
   % replicas in a PENDING state should inherit the state of a replica in the
   % prior configuration, while replicas from the prior configuration in a
   % WEDGED state can just update their configuration
   {Pending, Wedged} = lists:foldl(
      fun(Pid, {Pending, Wedged}) ->
            case libdist_utils:call(Pid, ping, Timeout) of
               {error, {elastic, pending}} ->
                  {[Pid | Pending], Wedged};
               {error, {elastic, wedged}} ->
                  {Pending, [Pid | Wedged]}
            end
      end,
      {[], []},
      NewReplicas),

   % since PENDING replicas can only inherit from wedged replicas in the old
   % configuration, do that first before unwedging wedged replicas
   libdist_utils:multicall(Pending, {1, {inherit, NextConf, OldConf}}, Timeout),
   libdist_utils:multicall(Wedged, {OldVn, {update_conf, NextConf}}, Timeout),
   {ok, NextConf}.    % return the new configuration

% Wedge a configuration
wedge(#conf{version = Vn, replicas = Replicas}, Timeout) ->
   libdist_utils:anycall(Replicas , {Vn, wedge}, Timeout);

% Wedge a particular process
wedge(Pid, Timeout) when is_pid(Pid) ->
   {ok, #conf{version = Vn}} = libdist_utils:call(Pid, get_conf, Timeout),
   libdist_utils:call(Pid, {Vn, wedge}, Timeout).


%%%%%%%%%%%%%%%%%%%%%
% Replica Callbacks %
%%%%%%%%%%%%%%%%%%%%%


% This is a replication protocol and it does not make use of extra arguments
type() -> ?REPL.
conf_args(_) -> [].

overloads(new) -> true;
overloads(reconfigure) -> true;
overloads(_) -> false.


% Send a command to a replicated object
cast(#conf{replicas=[Orderer | _], version=Vn, sm_mod = SMModule}, Command) ->
   Tag = case SMModule:is_mutating(Command) of
      false -> read;
      true -> write
   end,
   libdist_utils:cast(Orderer, {Vn, {Tag, Command}}).


% Initialize the state of a new replica
init_replica(_Me, Args) ->
   {Protocol, Timeout} = parse_args(Args),
   #elastic_state{
      unstable = ets:new(unstable_commands, []),
      protocol = Protocol,
      timeout  = Timeout
   }.


% Import a previously exported elastic state
import(ExportedState = #elastic_state{ unstable = UnstableList }) ->
   Unstable = ets:new(unstable_commands, []),
   ets:insert(Unstable, UnstableList),
   ExportedState#elastic_state{
      unstable = Unstable
   }.


% Export an elastic replica state
export(State = #elastic_state{unstable = Unstable}) ->
   State#elastic_state{
      unstable = ets:tab2list(Unstable)
   }.


% Update the protocol's custom state (due to replacement or reconfiguration)
update_state(Me, NewConf=#conf{version=Vn}, State=#elastic_state{protocol=P}) ->
   State#elastic_state{
      conf_version = Vn,
      pstate = P:update_state(Me, NewConf)
   }.


% Handle the failure of a replica
handle_failure(Me, Conf=#conf{version = Vn}, State, FailedPid, Info) ->
   ?SEND(Me, {Vn, {request_reconfig, Conf, FailedPid, Info}}, true),
   {Conf, State#elastic_state{mode = immutable}}.


% Handle a queued message

% Mode: PENDING
% A replica is in a pending state while it awaits transition to activation
% with a state that is either new (activate) or inherited from a prior
% configuration (inherit)
handle_msg(Me, Message, ASE = _AllowSideEffects, SM, State = #elastic_state{
      mode = pending,
      unstable = Unstable,
      timeout = Timeout
   }) ->
   case Message of
      % activate a configuration for a 'fresh' replicated object
      {Ref, Client, {1, {activate, NewConf}}} ->
         ?SEND(Me, {{Ref, Client}, Me, {reconfigure, NewConf}}, ASE),
         {consume, State#elastic_state{mode = waiting_for_reconfig}};

      % activate a configuration and inherit the state from a prior config
      {Ref, Client, {1, {inherit,
                          NewConf = #conf{version = NewVn},
                         _OldConf = #conf{version = OldVn, replicas = OldPids}
                      }}} when NewVn == OldVn + 1 ->

         {ok, {_Responder, {
                  ExportedState, UnstableList, StableCount, NextCmdNum
               } } } = libdist_utils:anycall(OldPids, {OldVn, inherit}, Timeout),

         InheritedSM = ldsm:import(ExportedState),
         ldsm:set_state(SM, ldsm:get_state(InheritedSM)),
         % XXX TODO: properly dispose of the "old SM state"
         ets:insert(Unstable, UnstableList),
         NewState = State#elastic_state{
            mode = waiting_for_reconfig,
            stable_count = StableCount,
            next_cmd_num = NextCmdNum
         },
         % update the configuration maintained by the replica
         ?SEND(Me, {{Ref, Client}, Me, {reconfigure, NewConf}}, ASE),
         {consume, NewState};

      % respond to all other queries with a 'pending' message
      {Ref, Client, _} ->
         Client ! {Ref, {error, {elastic, pending}}},
         consume
   end;


handle_msg(_Me, Message, ASE = _AllowSideEffects, _SM, State = #elastic_state{
   mode = waiting_for_reconfig
   }) ->
   case Message of
      {{OrigRef, OrigClient}, Reply} ->
         ?SEND(OrigClient, {OrigRef, Reply}, ASE),
         {consume, State#elastic_state{mode = active}};

      _ ->
         consume
   end;


% Mode: IMMUTABLE
% An immutable replica remains so for the rest of its life in this configuration.
% The only things it can do is pass down its history (stable + unstable) to a
% caller, or upgrade to a successor configuration.
handle_msg(Me, Message, ASE = _AllowSideEffects, SM, State = #elastic_state{
      mode = immutable,
      conf_version = Vn,
      unstable = Unstable,
      stable_count = StableCount,
      next_cmd_num = NextCmdNum
   }) ->
   case Message of
      % For practical considerations, a replica can inherit its history from a
      % prior configuration (i.e. upgrade its configuration).
      {Ref, Client, {Vn, {update_conf, NewConf=#conf{version=NewVn}
            }}} when NewVn == Vn + 1 ->
         % update the configuration maintained by the replica
         ?SEND(Me, {{Ref, Client}, Me, {reconfigure, NewConf}}, ASE),
         {consume, State#elastic_state{mode = waiting_for_reconfig}};

      % Request to inherit local state by a pending replica
      {Ref, Replica, {Vn, inherit}} ->
         % TODO: background state transfer
         CurrState = {
            ldsm:export(SM),
            ets:tab2list(Unstable),
            StableCount,
            NextCmdNum
         },
         ?SEND(Replica, {Ref, CurrState}, ASE),
         consume;

      % Request reconfiguration if in an Elastic Band
      {Vn, {request_reconfig, Config, FailedPid, FailureInfo}} ->
         % FIXME XXX: This signals reconfiguration request regardless of nested
         % protocol. Should I check to make sure it is an Elastic Band?
         case ldsm:is_rp_protocol(SM) of
            true ->
               ldsm:do(SM, {reconfig_needed, Config, FailedPid, FailureInfo}, true);
            _ ->
               do_nothing
         end,
         consume;

      % for other tagged  messages, return a 'wedged' message
      {Ref, Client, _} ->
         ?SEND(Client, {Ref, {error, {elastic, wedged}}}, ASE),
         consume;

      % ignore everything else
      _ ->
         consume
   end;


% Mode: ACTIVE
% When a replica is in an active state it can add commands to its history and
% respond to client requests. A replica stays in active state until it is wedged
handle_msg(Me, Message, ASE = _AllowSideEffects, SM, State = #elastic_state{
      mode = active,
      protocol = P,
      conf_version = Vn
   }) ->
   case Message of
      {Ref, Client, {Vn, wedge}} ->
         ?SEND(Client, {Ref, wedged}, ASE),
         {consume, State#elastic_state{mode = immutable}};

      {Ref, Client, {V, _}} when is_integer(V) ->
         if
            V < Vn ->
               ?SEND(Client, {Ref, {error, {elastic, reconfigured}}}, ASE),
               consume;
            V == Vn ->
               P:handle_msg(Me, Vn, Message, ASE, SM, State);
            true ->
               keep
         end;

      {V, _ProtocolMessage} when is_integer(V) ->
         if
            V < Vn  -> consume;    % ignore old messages
            V == Vn -> P:handle_msg(Me, Vn, Message, ASE, SM, State);
            true    -> keep
         end;

      _ ->
         no_match
   end.


%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%

parse_args(Args) ->
   Protocol = case proplists:is_defined(primary_backup, Args) of
      true -> elastic_primary_backup;
      false -> elastic_chain
   end,
   Timeout = proplists:get_value(timeout, Args, ?E_TO),
   {Protocol, Timeout}.
