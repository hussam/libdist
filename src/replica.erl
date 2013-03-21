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

is_mutating(_) ->
   true.

stop(#state{sm= SM}, Reason) ->
   ldsm:stop(SM, Reason).

export(State = #state{sm = SM, conf=#conf{protocol=P}, pstate = PState}) ->
   State#state{
      sm = ldsm:export(SM),
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
      {Ref, Client, {reconfigure, NewConf}} ->
         Client ! {Ref, ok},
         #conf{replicas = NewReplicas, partitions = NewPartitions} = NewConf,
         InNextConf = (
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
      {Ref, Client, {replace, Me, NewConf}} ->
         case ConfType of
            ?SINGLE ->
               Client ! {Ref, NewConf},
               consume;
            _ ->
               % update configuration trees locally and compute a new root conf
               {NewState, NewRootConf} = replace_replica(State, Me, NewConf),
               Client ! {Ref, NewRootConf},
               {consume, NewState}
         end;

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
         case ConfType of
            ?SINGLE -> Client ! {Ref, ldsm:module(SM)};  % no need to nest singletons
            _ -> Client ! {Ref, ?MODULE}
         end,
         consume;

      % Return the state machine's state
      % TODO: change this into some sort of background state transfer
      {Ref, Client, get_sm} ->
         case ConfType of
            ?SINGLE ->  % no need to nested protocols under singleton (performance)
               Client ! {Ref, ldsm:export(SM)};
            _ ->
               Client ! {Ref, ldsm:export(?MODULE, export(State))}
         end,
         consume;

      {Ref, Client, get_tags} ->
         Client ! {Ref, get_tags(State, [])},
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
      pstate = PState,
      conf = Conf = #conf{
         type = ConfType,
         protocol = P,
         version = Vn,
         replicas = Replicas,
         partitions = Partitions
      }
   }, OldReplica, NewReplica) ->

   % modify the configuration with the new list of replicas or partitions
   NewConf = case ConfType of
      ?REPL ->
         Conf#conf{
            version = Vn+1,
            replicas = libdist_utils:list_replace(
               OldReplica, NewReplica, Replicas)
         };

      ?PART ->
         {Tag, _} = lists:keyfind(OldReplica, 2, Partitions),
         Conf#conf{
            version = Vn+1,
            partitions = lists:keyreplace(
               OldReplica, 2, {Tag, NewReplica}, Partitions)
         }
   end,

   % if needed, notify siblings in RP Tree of configuration change. This is
   % only triggered by the actual process being replaced and nobody else
   case Me == OldReplica of
      true ->
         % trigger replacement/reconfiguration on other replicas/partitions
         Others = case ConfType of
            ?REPL ->
               lists:delete(OldReplica, Replicas);
            ?PART ->
               [Partition || {_,Partition} <- lists:keydelete(Me, 2, Partitions)]
         end,
         [ ?SEND(X, {replace, Me, NewReplica}, true) || X <- Others ],

         % update higher levels of the RP-Tree if they exist
         NewRootConf = case ldsm:is_rp_protocol(SM) of
            false -> NewConf;
            true -> element(2, replace_replica(ldsm:state(SM), Conf, NewConf))
         end,

         % update protocol state and create new replica/partition state
         NewState = State#state{
            me = NewReplica,
            conf = NewConf,
            pstate = P:update_state(NewReplica, NewConf, PState)
         },

         % return the new state and the new root configuration
         {NewState, NewRootConf};

      false ->
         % return the updated state
         State#state{
            conf = NewConf,
            pstate = P:update_state(Me, NewConf, PState)
         }
   end.


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
         get_tags(ldsm:state(SM), NewTags)
   end.
