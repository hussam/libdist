-module(replica).
-behaviour(ldsm).
-compile({inline, [handle_msg/4]}).

% This is a behaviour
-export([behaviour_info/1]).

% Public interface
-export([
      new/3
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
      {cast, 3},
      {init_replica, 1},
      {import, 1},
      {export, 1},
      {export, 2},
      {update_state, 3},
      {handle_msg, 5}
   ];
behaviour_info(_) ->
   undefined.


% Start a new replica
new(Protocol, {SMModule, SMArgs}, Node) ->
   server:start(Node, ?MODULE, {new, Protocol, SMModule, SMArgs});
% Start a new replica that will later inherit the state of an existing process
new(Protocol, no_sm, Node) ->
   server:start(Node, ?MODULE, {no_sm, Protocol}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%
% State Machine Callbacks %
%%%%%%%%%%%%%%%%%%%%%%%%%%%


init_sm(_) ->
   exit({not_implemented, "init_sm not meaningful for the replica module."}).

handle_cmd(State = #state{me = Me}, Message, AllowSideEffects) ->
   case handle_msg(Me, Message, AllowSideEffects, State) of
      {_, NewState} -> {noreply, NewState};
      _ -> noreply
   end.

is_mutating({_Ref, _Client, _RId, {read, _Command}}) ->
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
      pstate = P:export(PState, Tag)
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

init(Me, {new, PModule, SMModule, SMArgs}) ->
   #state{
      me = Me,
      sm = ldsm:start(SMModule, SMArgs),
      conf = #conf{type = PModule:type(), protocol=PModule},
      pstate = PModule:init_replica(Me)
   };
init(Me, {no_sm, PModule}) ->
   #state{
      me = Me,
      conf = #conf{type = PModule:type(), protocol=PModule},
      pstate = PModule:init_replica(Me)
   }.


% Handle a queued message
handle_msg(Me, Message, State) ->
   handle_msg(Me, Message, true, State).


% Handle a queued message
handle_msg(Me, Message, ASE = _AllowSideEffects, State = #state{
      sm = SM,
      conf = Conf = #conf{type = ConfType, protocol = P},
      pstate = PState
   }) ->
   case Message of
      % Change this replica's configuration
      % TODO: handle reconfiguration in nested protocols
      {Ref, Client, _RId, {reconfigure, NewConf}} ->
         Client ! {Ref, ok},
         #conf{replicas = NewReplicas, partitions = NewPartitions} = NewConf,
         InNextConf = (
            ((ConfType == ?SINGLE) and (NewReplicas == [Me])) or
            ((ConfType == ?REPL) and lists:member(Me, NewReplicas)) or
            ((ConfType == ?PART) and lists:keymember(Me, 2, NewPartitions))
         ),
         case InNextConf of
            true ->
               {consume, State#state{
                     conf = NewConf,
                     pstate = P:update_state(Me, NewConf, PState)
                  }};
            false ->
               ldsm:stop(SM,reconfigure),
               {stop, reconfigure}
         end;

      % update the configuration by replacing OldReplica with NewReplica.
      {Ref, Client, _RId, {replace, Me, NewConf}} ->
         % update configuration trees locally and notify others
         ExportedState = State#state{sm = ldsm:export(SM)},
         {_, NewRootConf} = replace_replica(ExportedState, Me, NewConf, true),
         ldsm:stop(SM, {replaced, Me, NewConf}),
         Client ! {Ref, NewRootConf},
         {stop, replaced};

      % update the configuration by replacing OldReplica with NewReplica
      {replace, OldReplica, NewReplica} ->
         NewConf = update_conf_members(Conf, OldReplica, NewReplica),
         % return the updated state
         NewState = State#state{
            conf = NewConf,
            pstate = P:update_state(Me, NewConf, PState)
         },
         {consume, NewState};

      % Stop this replica
      {Ref, Client, _RId, {stop, Reason}} ->
         Client ! {Ref, ldsm:stop(SM, Reason)},
         {stop, Reason};

      % Return the configuration at this replica
      {Ref, Client, _RId, get_conf} ->
         Client ! {Ref, Conf},
         consume;

      {Ref, Client, _RId, {inherit_sm, Pid, Coverage, Retry}} ->
         PidSM = libdist_utils:call(Pid, {get_sm, Coverage}, Retry),
         PidSMState = ldsm:get_state(PidSM),
         {NewSMState, NewRootConf} = replace_replica(PidSMState, Pid, Conf, false),
         NewSM = ldsm:import(ldsm:set_state(PidSM, NewSMState)),
         Client ! {Ref, NewRootConf},
         {consume, State#state{sm = NewSM}};

      % Return the state machine's module
      {Ref, Client, _RId, get_sm_module} ->
         case ConfType of
            ?SINGLE -> Client ! {Ref, ldsm:module(SM)};  %do not nest singletons
            _ -> Client ! {Ref, ?MODULE}
         end,
         consume;

      % Added for debugging purposes only
      {Ref, Client, RId, get_sm} ->
         handle_msg(Me, {Ref, Client, RId, {get_sm, all}}, ASE, State);

      % Return the state machine's state
      % TODO: change this into some sort of background state transfer
      {Ref, Client, _RId, {get_sm, Coverage}} ->
         case {ConfType, Coverage} of
            % no need to nest protocols under singletons (performance)
            {?SINGLE, all} ->
               Client ! {Ref, ldsm:export(SM)};
            {?SINGLE, {part, Tag}} ->
               Client ! {Ref, ldsm:export(SM, Tag)};
            {_, all} ->
               Client ! {Ref, ldsm:wrap(?MODULE, export(State))};
            {_, {part, Tag}} ->
               Client ! {Ref, ldsm:wrap(?MODULE, export(State, Tag))}
         end,
         consume;

      {Ref, Client, _RId, get_tags} ->
         Client ! {Ref, get_tags(State#state{sm = ldsm:export(SM)}, [])},
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


% TODO: XXX: This expects an exported state. FIXME!!
replace_replica(State = #state{
      me = Me,
      sm = SM,
      pstate = PState,
      conf = Conf = #conf{
         type = ConfType,
         protocol = P,
         replicas = Replicas,
         partitions = Partitions
      }
   }, OldReplica, NewReplica, DoNotify) ->
   % if needed, notify siblings in RP Tree of configuration change. This should
   % only be triggered by the actual process being replaced and nobody else
   case Me == OldReplica of
      false ->
         error(replace_replica_should_not_get_here);
      true ->
         % trigger replacement/reconfiguration on other replicas/partitions
         Others = case ConfType of
            ?SINGLE ->
               [];
            ?REPL ->
               lists:delete(OldReplica, Replicas);
            ?PART ->
               [Partition || {_,Partition} <- lists:keydelete(Me, 2, Partitions)]
         end,
         [ ?SEND(X, ?ALL, {replace, Me, NewReplica}, DoNotify) || X <- Others ],

         % modify the configuration with the new list of replicas or partitions
         NewConf = update_conf_members(Conf, OldReplica, NewReplica),

         % update higher levels of the RP-Tree if they exist
         {NewSMState, NewRootConf} = replace_replica(
            ldsm:get_state(SM), Conf, NewConf, DoNotify),

         % update protocol state and create new replica/partition state
         NewState = State#state{
            me = NewReplica,
            sm = ldsm:set_state(SM, NewSMState),
            conf = NewConf,
            pstate = P:update_state(NewReplica, NewConf, PState)
         },

         % return the new state and the new root configuration
         {NewState, NewRootConf}
   end;

% in case State does not correspond to an R/P protocol
replace_replica(State, _OldReplica, NewReplica, _DoNotify) -> 
   {State, NewReplica}.



% Recursively get the partitioning tags associated with current state machine
% TODO: XXX: This expects an exported state. FIXME!!
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
update_conf_members(OldConf = #conf{
      type = ConfType, version=Vn, replicas = Replicas, partitions = Partitions
   }, OldMember, NewMember) ->
   case ConfType of
      ?REPL ->
         OldConf#conf{
            version = Vn + 1,
            replicas = libdist_utils:list_replace(OldMember, NewMember, Replicas)
         };

      ?PART ->
         {Tag, _} = lists:keyfind(OldMember, 2, Partitions),
         OldConf#conf{
            version = Vn+1,
            partitions = lists:keyreplace(
               OldMember, 2, Partitions, {Tag, NewMember})
         };

      ?SINGLE ->
         NewMember

   end.


