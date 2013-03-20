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
      import/1
   ]).

% Server callbacks
-export([
      init/2,
      handle_msg/3
   ]).

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
      {make_conf_args, 2},
      {cast, 2},
      {init_replica, 1},
      {import, 1},
      {export, 1},
      {update_state, 3},
      {handle_msg, 5}
   ];
behaviour_info(_) ->
   undefined.


% Start a new replica
new({Protocol, _ProtocolArgs}, {SMModule, SMArgs}, Node) ->
   server:start(Node, ?MODULE, {new, Protocol, SMModule, SMArgs});
% Start a new replica that will later inherit the state of an existing process
new(no_sm, {Protocol, _ProtocolArgs}, Node) ->
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

is_mutating(_) ->
   true.

stop(#state{sm= SM}, Reason) ->
   ldsm:stop(SM, Reason).

export(State = #state{sm = SM, conf=#rconf{protocol=P}, pstate = PState}) ->
   State#state{
      sm = ldsm:export(SM),
      pstate = P:export(PState)
   }.

import(State=#state{sm=ExportedSM, conf=#rconf{protocol=P}, pstate=ExportedPState}) ->
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
      conf = #rconf{protocol = PModule},
      pstate = PModule:init_replica(Me)
   };
init(Me, {no_sm, PModule}) ->
   #state{
      me = Me,
      conf = #rconf{protocol = PModule},
      pstate = PModule:init_replica(Me)
   }.


% Handle a queued message
handle_msg(Me, Message, State) ->
   handle_msg(Me, Message, true, State).


% Handle a queued message
handle_msg(Me, Message, ASE = _AllowSideEffects, State = #state{
      sm = SM,
      conf = Conf = #rconf{protocol = P},
      pstate = PState
   }) ->
   case Message of
      % Change this replica's configuration
      % TODO: handle reconfiguration in nested protocols
      {Ref, Client, {reconfigure, NewConf=#rconf{pids=NewReplicas}}} ->
         Client ! {Ref, ok},
         case lists:member(Me, NewReplicas) of
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
      {Ref, Client, {replace, OldReplica, NewReplica}} ->
         % update configuration trees locally and compute the new root conf
         {NewState, NewRootConf} = replace_replica(State, OldReplica, NewReplica),
         Client ! {Ref, NewRootConf},
         {consume, NewState};

      % update the configuration by replacing OldReplica with NewReplica
      {replace, OldReplica, NewReplica} ->
         {consume, replace_replica(State, OldReplica, NewReplica)};

      % Stop this replica
      {Ref, Client, {stop, Reason}} ->
         ?SEND(Client, {Ref, ldsm:stop(SM, Reason)}, ASE),
         {stop, Reason};

      % Return the configuration at this replica
      {Ref, Client, get_conf} ->
         ?SEND(Client, {Ref, Conf}, ASE),
         consume;

      {Ref, Client, {inherit_sm, Pid, Retry}} ->
         NewSM = ldsm:import(libdist_utils:call(Pid, get_sm, Retry)),
         Client ! {Ref, ok},
         {consume, State#state{sm = NewSM}};

      % Return the state machine's module
      {Ref, Client, get_sm_module} ->
         Client ! {Ref, ?MODULE},
         consume;

      % Return the state machine's state
      % TODO: change this into some sort of background state transfer
      {Ref, Client, get_sm} ->
         Client ! {Ref, ldsm:export(?MODULE, export(State))},
         consume;

      % Punt all other messages to the protocol's handler
      _ ->
         case P:handle_msg(Me, Message, ASE, SM, PState) of
            {consume, NewPState} ->
               {consume, State#state{pstate = NewPState}};

            {keep, NewPState} ->
               {consume, State#state{pstate = NewPState}};

            Other ->
               Other
         end
   end.



%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%


replace_replica(State = #state{
      me = Me,
      sm = SM,
      conf = Conf = #rconf{protocol = P, version = Vn, pids = Replicas},
      pstate = PState
   }, OldReplica, NewReplica) ->

   % if needed, notify siblings in RP Tree of configuration change. This is
   % only triggered by the actual process being replaced and nobody else
   case Me == OldReplica of
      true ->
         % trigger replacement/reconfiguration on other replicas
         Others = lists:delete(OldReplica, Replicas),
         [ ?SEND(R, {replace, OldReplica, NewReplica}, true) || R <- Others ],

         NewReps = libdist_utils:list_replace(OldReplica, NewReplica, Replicas),
         NewConf = Conf#rconf{version = Vn+1, pids = NewReps},

         % update higher levels of the RP-Tree if they exist
         NewRootConf = case ldsm:is_rp_protocol(SM) of
            false -> NewConf;
            true -> element(2, replace_replica(ldsm:state(SM), Conf, NewConf))
         end,

         % update protocol state and create new replica state
         NewState = State#state{
            me = NewReplica,
            conf = NewConf,
            pstate = P:update_state(NewReplica, NewConf, PState)
         },

         % return the new state and the new root configuration
         {NewState, NewRootConf};

      false ->
         % modify the configuration and return the updated state
         NewReps = libdist_utils:list_replace(OldReplica, NewReplica, Replicas),
         NewConf = Conf#rconf{version = Vn+1, pids = NewReps},
         State#state{
            conf = NewConf,
            pstate = P:update_state(Me, NewConf, PState)
         }
   end.

