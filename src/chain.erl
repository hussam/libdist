-module(chain).
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

-include("constants.hrl").
-include("helper_macros.hrl").
-include("libdist.hrl").

% State specific to a chain replica
-record(chain_state, {
      previous,
      next,
      unstable,
      stable_count = 0,
      next_cmd_num = 0
   }).

-define(KEYPOS, 1).

%%%%%%%%%%%%%%%%%%%%%
% Replica Callbacks %
%%%%%%%%%%%%%%%%%%%%%


% This is a replication protocol and it does not make use of extra arguments
% and it does not overload any generic repobj functions
type() -> ?REPL.
conf_args(Args) -> Args.
overloads(_) -> false.


% Send an asynchronous command to a chain replicated object
cast(#conf{replicas = Reps = [Head | _], sm_mod = SMModule}, Command) ->
   {Tag, Target} = case SMModule:is_mutating(Command) of
      true -> {write, Head};
      false -> {read, lists:last(Reps)}
   end,
   libdist_utils:cast(Target, {Tag, Command}).


% Initialize the state of a new replica
init_replica(_Me, _Args) ->
   #chain_state{
      unstable = ets:new(unstable_commands, [{keypos, ?KEYPOS}])
   }.


% Import a previously exported chain state
import(ExportedState = #chain_state{ unstable = UnstableList }) ->
   Unstable = ets:new(unstable_commands, [{keypos, ?KEYPOS}]),
   ets:insert(Unstable, UnstableList),
   ExportedState#chain_state{
      unstable = Unstable
   }.


% Export a chain replica state
export(State = #chain_state{unstable = Unstable}) ->
   State#chain_state{
      unstable = ets:tab2list(Unstable)
   }.


% Update the protocol's custom state (due to replacement or reconfiguration)
update_state(Me, #conf{replicas = NewReps}, State) ->
   {_, NewPrev, NewNext} = libdist_utils:ipn(Me, NewReps),
   State#chain_state{
      next = NewNext,
      previous = NewPrev
   }.


% Handle the failure of a replica
handle_failure(Me, NewConf, State, _FailedPid, _Info) ->
   update_state(Me, NewConf, State).


% Handle a queued message
handle_msg(_Me, Message, ASE = _AllowSideEffects, SM, State = #chain_state{
      previous = Prev,
      next = Next,
      unstable = Unstable,
      stable_count = StableCount,
      next_cmd_num = NextCmdNum
   }) ->
   case Message of
      % Handle command as the HEAD of the chain
      {Ref, Client, {write, Command}} when Prev == chain_head ->
         FwdMsg = {NextCmdNum, Ref, Client, Command},
         ets:insert(Unstable, FwdMsg),
         ?SEND(Next, FwdMsg, ASE),
         {consume, State#chain_state{next_cmd_num = NextCmdNum+ 1}};

      % Handle command as any replica in the MIDDLE of the chain
      {NextCmdNum, _Ref, _Client, _Cmd} when Next /= chain_tail ->
         ?SEND(Next, Message, ASE),
         ets:insert(Unstable, Message),
         {consume, State#chain_state{next_cmd_num = NextCmdNum + 1}};

      % Handle update command as the TAIL of the chain
      {NextCmdNum, Ref, Client, Command} ->
         ldsm:do(SM, Ref, Client, Command, ASE),
         ?SEND(Prev, {stabilized, NextCmdNum}, ASE),
         C = NextCmdNum + 1,
         {consume, State#chain_state{next_cmd_num = C, stable_count = C}};

      % Handle query command as the TAIL of the chain
      {Ref, Client, {read, Command}} ->
         ldsm:do(SM, Ref, Client, Command, ASE),
         consume;

      {stabilized, StableCount} ->
         [{StableCount, _, _, Command}] = ets:lookup(Unstable, StableCount),
         ldsm:do(SM, Command, false),
         if
            Prev /= chain_head -> ?SEND(Prev, Message, ASE);
            true -> do_not_forward
         end,
         ets:delete(Unstable, StableCount),
         {consume, State#chain_state{stable_count = StableCount + 1}};

      _ ->
         no_match
   end.
