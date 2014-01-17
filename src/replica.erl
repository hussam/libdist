-module(replica).
-behaviour(ldsm).
-compile({inline, [handle_msg/4, in_conf/2, get_peers/2, reconf_sm/3]}).

% This is a behaviour
-export([behaviour_info/1]).

% Public interface
-export([
      new/4
   ]).

% State machine interface
-export([
      init_sm/1,
      handle_cmd/3,
      is_mutating/1,
      stop/2,
      export/1,
      export/2,
      import/1
   ]).

% Server callbacks
-export([
      init/2,
      handle_msg/3
   ]).

-include("constants.hrl").
-include("helper_macros.hrl").
-include("libdist.hrl").

-record(state, {
      me,
      sm,
      conf,
      pstate
   }).


% Define behaviour callbacks
behaviour_info(callbacks) ->
   [
      {type, 0},
      {conf_args, 1},
      {overloads, 1},
      {cast, 2},
      {init_replica, 2},
      {import, 1},
      {export, 1},
      {update_state, 3},
      {handle_failure, 5},
      {handle_msg, 5}
   ];
behaviour_info(_) ->
   undefined.


% Start a new replica
new(Protocol, ProtocolArgs, {SMModule, SMArgs}, Node) ->
   server:start(Node, ?MODULE, {new, Protocol, ProtocolArgs, SMModule, SMArgs});
% Start a new replica that will later inherit the state of an existing process
new(Protocol, ProtocolArgs, no_sm, Node) ->
   server:start(Node, ?MODULE, {no_sm, Protocol, ProtocolArgs}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%
% State Machine Callbacks %
%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Replicas implement the state machine (ldsm) interface only so that they can be
% nested within one another. Replicas are nested by letting an existing replica
% process import the wrapped exported state of another replica process.

init_sm(_) ->  % not used; nested replica ldsms are created via wrap() not new()
   exit({not_implemented, "init_sm not meaningful for the replica module."}).

handle_cmd(State = #state{me = Me}, Message, AllowSideEffects) ->
   case handle_msg(Me, Message, AllowSideEffects, State) of
      {_, NewState} -> {noreply, NewState};
      _ -> noreply
   end.

is_mutating({_Ref, _Client, {read, _Command}}) ->
   false;
is_mutating(_) ->
   true.

stop(#state{sm= SM}, Reason) ->
   ldsm:stop(SM, Reason).

export(State = #state{sm = SM, conf=#conf{protocol=P}, pstate = PState}) ->
   State#state{
      sm = ldsm:export(SM),
      pstate = P:export(PState)
   }.

export(State = #state{sm = SM, conf=#conf{protocol=P}, pstate = PState}, Tag) ->
   State#state{
      sm = ldsm:export(SM, Tag),
      pstate = P:export(PState)
   }.

import(State=#state{sm=ExportedSM, conf=#conf{protocol=P}, pstate=ExportedPState}) ->
   State#state{
      sm = ldsm:import(ExportedSM),
      pstate = P:import(ExportedPState)
   }.


%%%%%%%%%%%%%%%%%%%%
% Server Callbacks %
%%%%%%%%%%%%%%%%%%%%


% Initialize the state of a new replica

init(Address, {new, PModule, PArgs, SMModule, SMArgs}) ->
   #state{
      me = Address,
      sm = ldsm:start(SMModule, SMArgs),
      conf = #conf{type = PModule:type(), protocol=PModule, replicas=[Address]},
      pstate = PModule:init_replica(Address, PArgs)
   };
init(Address, {no_sm, PModule, PArgs}) ->
   #state{
      me = Address,
      conf = #conf{type = PModule:type(), protocol=PModule, replicas=[Address]},
      pstate = PModule:init_replica(Address, PArgs)
   }.


% Handle a queued message
handle_msg(Address, Message, State) ->
   handle_msg(Address, Message, true, State).


% Handle a queued message
handle_msg(_Address, Message, ASE = _AllowSideEffects, State = #state{
      me = Me,
      sm = SM,
      conf = Conf = #conf{type = ConfType, protocol = P, version = ConfVn},
      pstate = PState
   }) ->
   case Message of
      {'DOWN', _MonitorRef, process, _FailedPid, {replaced, _, #conf{}}} ->
         % ignore failure notifications due to processes being replaced in the
         % RP-Tree
         consume;

      {'DOWN', _MonitorRef, process, FailedPid, Info} ->
         case { in_conf(FailedPid, Conf) , ldsm:is_rp_protocol(SM) } of
            {false, true} ->  % Failed process is an uncle in the RP-Tree
               ldsm:do(SM, Message, ASE),
               consume;

            _ ->     % Failed process is a peer of the current replica
               % let the protocol update its state to respond to the failure
               {NewConf, NewPState} = P:handle_failure(
                                          Me, Conf, PState, FailedPid, Info),
               % update the local state and configuration accordingly
               NextConf = case NewConf /= Conf of
                  true -> NewConf#conf{version = ConfVn + 1};
                  false -> Conf
               end,
               NewState = State#state{conf = NextConf, pstate = NewPState},
               reconf_sm(SM, Conf, NextConf),
               {consume, NewState}
         end;

      % Change this replica's configuration
      % TODO: handle reconfiguration in nested protocols
      {Ref, Client, {reconfigure, NewConf}} ->
         Client ! {Ref, ok},
         case in_conf(Me, NewConf) of
            true ->
               % monitor peers in new configuration if they are processes
               [monitor(process, Peer) || Peer <- get_peers(Me, NewConf), is_pid(Peer)],

               reconf_sm(SM, Conf, NewConf),

               {consume, State#state{
                     conf = NewConf,
                     pstate = P:update_state(Me, NewConf, PState)
                  }};
            false ->
               ldsm:stop(SM, reconfigure),
               {stop, reconfigure}
         end;

      % update the configuration by replacing self with a new configuration
      {Ref, Client, {replace, Me, NewConf=#conf{shard_agent = NewShardAgent}}}  ->
         OldInnermostSM = get_innermost_sm(SM),
         % update configuration trees locally and notify others
         case NewShardAgent of
            ?NoSA ->
               replace_replica(Ref, Client, State, Me, NewConf, true),
               ldsm:stop(OldInnermostSM, {replaced, Me, NewConf}),
               {stop, {replaced, Me, NewConf}};

            Me ->
               NewState = replace_replica(Ref, Client,
                  State#state{sm = replace_innermost_sm(SM,
                        ldsm:start(shard_agent, NewConf))},
                  Me, NewConf, true),

               ldsm:stop(OldInnermostSM, {replaced, Me, NewConf}),
               {consume, NewState};

            _ ->
               error({invalid_shard_agent_in_replacement_conf, Me, NewShardAgent})
         end;

      % update the configuration at a shard agent
      {replace_shardagent, Me, NewConf, Ref, Client} ->
         ShardAgent = get_innermost_sm(SM),
         ldsm:set_state(ShardAgent, NewConf),
         NewState = replace_replica(Ref, Client, State, Me, NewConf, true),
         {consume, NewState};

      % update the configuration by replacing OldReplica with NewReplica
      {replace, OldReplica, NewReplica} ->
         NewConf = replace_conf_member(Conf, OldReplica, NewReplica),
         % return the updated state
         NewState = State#state{
            conf = NewConf,
            pstate = P:update_state(Me, NewConf, PState)
         },
         {consume, NewState};


      % Stop this replica
      {Ref, Client, {stop, Reason}} ->
         Client ! {Ref, ldsm:stop(SM, Reason)},
         {stop, Reason};

      % Return the configuration at this replica
      {Ref, Client, get_conf} ->
         Client ! {Ref, Conf},
         consume;

      {Ref, Client, {inherit_sm, Pid, Intent, _Retry}} ->
         {ok, PidSM} = state_transfer:request_blocking(Pid, {get_sm, Intent}),
         NewSMState = replace_replica(ldsm:get_state(PidSM), Pid, Conf, false),
         monitor_nested_siblings(NewSMState),
         NewSM = ldsm:import(ldsm:set_state(PidSM, NewSMState)),
         ldsm:reconfigure(NewSM, Conf),
         Client ! {Ref, ok},
         {consume, State#state{sm = NewSM}};


      % Return the state machine's module
      {Ref, Client, get_sm_module_and_conf_type} ->
         Mod = case ConfType of
            ?SINGLE -> ldsm:module(SM);      %do not nest singletons
            _ -> ?MODULE
         end,
         Client ! {Ref, {Mod, ConfType}},
         consume;

      % Return the state machine's state
      {{get_sm, Intent}, STInfo} ->
         TransferState = case Intent of
            replicate when ConfType == ?SINGLE -> ldsm:export(SM);
            replicate -> ldsm:wrap(?MODULE, export(State));
            {partition, Tag} -> ldsm:export(get_innermost_sm(SM), Tag)
         end,
         state_transfer:serve_nonblocking(
            STInfo,
            TransferState,
            ?MTU,
            fun(_)-> ok end      % Dummy callback. TODO handle potential errors
         ),
         consume;

      {Ref, Client, get_tags} ->
         Client ! {Ref, get_tags(State, [])},
         consume;


      % Added for debugging purposes only
      {Ref, Client, {dbg, get_state}} ->
         Client ! {Ref, ldsm:wrap(?MODULE, export(State))},
         consume;


      % Punt all other messages to the protocol's handler
      _ ->
         case P:handle_msg(Me, Message, ASE, SM, PState) of
            {consume, NewPState} ->
               {consume, State#state{pstate = NewPState}};

            {keep, NewPState} ->
               {keep, State#state{pstate = NewPState}};

            Other ->
               Other
         end
   end.



%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%


% Set up monitoring for peers of nested replicas on this process
monitor_nested_siblings(#state{me = Me, sm = SM, conf = Conf}) ->
   [ monitor(process, P) || P <- get_peers(Me, Conf), is_pid(P) ],
   monitor_nested_siblings(ldsm:get_state(SM));
monitor_nested_siblings(_) -> [].


% Locally replace OldReplica with NewReplica and potentially notify peers
replace_replica(State, OldReplica, NewReplica, DoNotify) ->
   replace_replica([], noreply, State, OldReplica, NewReplica, DoNotify).

replace_replica(Ref, Client, State, OldReplica, NewReplica, DoNotify) ->
   {NewState, Ret} = do_replace_replica(State, OldReplica, NewReplica, DoNotify),
   case Ret of
      noreply ->
         do_nothing;

      {replace_shardagent, ShardAgent, Conf, NewConf} ->
         ?SEND(ShardAgent,
            {replace_shardagent, Conf, NewConf, Ref, Client}, true);

      NewRootConf when Client /= noreply ->
         Client ! {Ref, NewRootConf};

      _ ->
         do_nothing
   end,
   NewState.   % return the modified state


do_replace_replica(State = #state{
      me = Me,
      sm = SM,
      pstate = PState,
      conf = Conf = #conf{
         protocol = P,
         shard_agent = ShardAgent
      }
   }, OldReplica, NewReplica, DoNotify) ->
   % if needed, notify siblings in RP Tree of configuration change. This should
   % only be triggered by the actual process being replaced and nobody else
   case Me == OldReplica of
      false ->
         error(replace_replica_should_not_get_here);
      true ->
         % trigger replacement/reconfiguration on other replicas/partitions
         Others = get_peers(Me, Conf),
         [ ?SEND(X, {replace, Me, NewReplica}, DoNotify) || X <- Others ],

         % modify the configuration with the new list of replicas or partitions
         NewConf = replace_conf_member(Conf, OldReplica, NewReplica),

         % update protocol state and create new replica/partition state
         NewState = State#state{
            me = NewReplica,
            conf = NewConf,
            pstate = P:update_state(NewReplica, NewConf, PState)
         },


         % update higher levels of the RP-Tree
         case ShardAgent of
            ?NoSA ->
               {NewSMState, NewRootConf} = do_replace_replica(
                  ldsm:get_state(SM), Conf, NewConf, DoNotify),

               % update the nested state, and return the new state and root conf
               {NewState#state{sm=ldsm:set_state(SM, NewSMState)}, NewRootConf};

            _ when DoNotify ->
               {NewState, {replace_shardagent, ShardAgent, Conf, NewConf}};

            _ ->
               {NewState, noreply}
         end
   end;

% in case State does not correspond to an R/P protocol
do_replace_replica(State, _OldReplica, NewReplica, _DoNotify) -> 
   {State, NewReplica}.



% Recursively get the partitioning tags associated with current state machine
get_tags(#state{
      me = Me,
      sm = SM,
      conf = #conf{ type = ConfType , partitions = Partitions }
   }, Tags) ->
   % get the tags associated with the current configuration
   NewTags = case ConfType of
      ?PART -> {Tag, _} = lists:keyfind(Me, 2, Partitions), [Tag | Tags];
      _ -> Tags
   end,
   % get the tags associated with higher rp-tree nodes
   case ldsm:is_rp_protocol(SM) of
      false -> % top-level rp-tree node, return current tags
         lists:reverse(NewTags);
      true ->
         get_tags(ldsm:get_state(SM), NewTags)
   end.


% Create a new configuration based on OldConf where OldMember is replaced by
% NewMember and all other settings (except the version number) stay the same
replace_conf_member(OldConf = #conf{
      type = ConfType, version=Vn, replicas = Replicas, partitions = Partitions
   }, OldMember, NewMember) ->
   case in_conf(OldMember, OldConf) of
      false ->
         OldConf;
      true ->
         case ConfType of
            ?PART ->
               {Tag, _} = lists:keyfind(OldMember, 2, Partitions),
               OldConf#conf{
                  version = Vn+1,
                  partitions = lists:keyreplace(
                     OldMember, 2, Partitions, {Tag, NewMember})
               };
            ?REPL ->
               OldConf#conf{
                  version = Vn + 1,
                  replicas = libdist_utils:list_replace(
                     OldMember, NewMember, Replicas)
               };
            ?SINGLE ->
               NewMember
         end
   end.


% Test whether a process or PSM is a member of a given configuration
in_conf(Elem, #conf{type=ConfType, replicas=Replicas, partitions=Partitions}) ->
   case ConfType of
      ?PART -> lists:keymember(Elem, 2, Partitions);
      _ -> lists:member(Elem, Replicas)
   end.


% Return the peers of a process or PSM in the given configuration
get_peers(Me, #conf{type=ConfType, replicas=Replicas, partitions=Partitions}) ->
   case ConfType of
      ?PART -> lists:delete(Me, [ P || {_Tag, P} <- Partitions ]);
      _ ->     lists:delete(Me, Replicas)
   end.


replace_innermost_sm(OldSM, NewSM) ->
   case ldsm:get_state(OldSM) of
      #state{sm = NestedSM} = State ->
         ldsm:set_state(OldSM,
            State#state{sm = replace_innermost_sm(NestedSM, NewSM)});
      _ ->
         NewSM
   end.


get_innermost_sm(SM) ->
   case ldsm:get_state(SM) of
      #state{sm = NestedSM} -> get_innermost_sm(NestedSM);
      _ -> SM
   end.

reconf_sm(SM, OldConf, NewConf) ->
   % propagate changes up the rp-tree (nested protocols) if needed
   case ldsm:is_rp_protocol(SM) of
      true when NewConf /= OldConf ->
         ldsm:set_state(SM, replace_replica(
               ldsm:get_state(SM), OldConf, NewConf, true) );
      _ ->
         do_nothing
   end,
   case SM of
      undefined -> do_nothing;
      _ -> ldsm:reconfigure(SM, NewConf)
   end,
   SM.

