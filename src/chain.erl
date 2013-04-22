-module(chain).
-behaviour(replica).

% replica callbacks
-export([
      type/0,
      conf_args/1,
      cast/3,
      init_replica/1,
      import/1,
      export/1,
      export/2,
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
      tags = [],
      counter = 0,
      next_cmd_nums = dict:from_list([ {[], 0} ]),
      stable_counts = dict:from_list([ {[], 0} ])
   }).

-define(KEYPOS, 1).

%%%%%%%%%%%%%%%%%%%%%
% Replica Callbacks %
%%%%%%%%%%%%%%%%%%%%%


% This is a partitioning protocol and it does not make use of extra arguments
type() -> ?REPL.
conf_args(Args) -> Args.


% Send an asynchronous command to a chain replicated object
cast(#conf{replicas = Reps = [Head | _], sm_mod = SMModule}, RId, Command) ->
   {Tag, Target} = case SMModule:is_mutating(Command) of
      true -> {write, Head};
      false -> {read, lists:last(Reps)}
   end,
   libdist_utils:cast(Target, RId, {Tag, Command}).


% Initialize the state of a new replica
init_replica(_Me) ->
   #chain_state{
      unstable = ets:new(unstable_commands, [{keypos, ?KEYPOS}])
   }.


% Import a previously exported chain state
import(ExportedState = #chain_state{
      unstable = UnstableList,
      stable_counts = StableCountsList,
      next_cmd_nums = NextCmdNumsList
   }) ->
   Unstable = ets:new(unstable_commands, [{keypos, ?KEYPOS}]),
   ets:insert(Unstable, UnstableList),
   ExportedState#chain_state{
      unstable = Unstable,
      stable_counts = dict:from_list(StableCountsList),
      next_cmd_nums = dict:from_list(NextCmdNumsList)
   }.


% Export a chain replica state
export(State=#chain_state{unstable=U, stable_counts=SC, next_cmd_nums=NCN}) ->
   State#chain_state{
      unstable = ets:tab2list(U),
      stable_counts = dict:to_list(SC),
      next_cmd_nums = dict:to_list(NCN)
   }.

% Export part of a chain replica's state
export(State = #chain_state{tags = OldTags}, NewTag) ->
   % TODO: implement this properly!!
   export(State#chain_state{tags = [NewTag | OldTags]}).


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
      tags = Tags,
      counter = Counter,
      stable_counts = StableCounts,
      next_cmd_nums = NextCmdNums
   }) ->
   case Message of
      % Handle command as the HEAD of the chain
      {Ref, Client, RId, {write, Command}} when Prev == chain_head ->
         CmdNum = {Tags, Counter},
         FwdMsg = {CmdNum, Ref, Client, RId, Command},
         ets:insert(Unstable, FwdMsg),
         ?SEND(Next, RId, FwdMsg, ASE),
         {consume, State#chain_state{counter = Counter + 1}};

      % Handle command as any replica in the MIDDLE of the chain
      {CmdNum, _Ref, _Client, RId, _Cmd} when Next /= chain_tail ->
         case libdist_utils:is_next_cmd(CmdNum, NextCmdNums) of
            {true, UpdatedNextCmdNums} ->
               ?SEND(Next, RId, Message, ASE),
               ets:insert(Unstable, Message),
               {consume, State#chain_state{next_cmd_nums = UpdatedNextCmdNums}};
            false ->
               keep
         end;

      % Handle update command as the TAIL of the chain
      {CmdNum, Ref, Client, RId, Command} ->
         case libdist_utils:is_next_cmd(CmdNum, NextCmdNums) of
            {true, UpdatedNextCmdNums} ->
               ldsm:do(SM, Ref, Client, Command, ASE),
               ?SEND(Prev, RId, {stabilized, CmdNum}, ASE),
               {consume, State#chain_state{next_cmd_nums = UpdatedNextCmdNums}};
            false ->
               keep
         end;

      % Handle query command as the TAIL of the chain
      {Ref, Client, _RId, {read, Command}} ->
         ldsm:do(SM, Ref, Client, Command, ASE),
         consume;

      {stabilized, CmdNum} ->
         case libdist_utils:is_next_cmd(CmdNum, StableCounts) of
            {true, NewStableCounts} ->
               case ets:lookup(Unstable, CmdNum) of
                  [{CmdNum, _, _, RId, Command}] ->
                     ldsm:do(SM, Command, false),
                     if
                        Prev /= chain_head -> ?SEND(Prev, RId, Message, ASE);
                        true -> do_not_forward
                     end,
                     ets:delete(Unstable, CmdNum),
                     {consume, State#chain_state{stable_counts = NewStableCounts}};
                  [] ->
                     % XXX: this is a temporary fix for partitioned state
                     % machines. Must find a real fix soon. FIXME!!!
                     keep     % ignore it
               end;
            false ->
               keep
         end;

      _ ->
         no_match
   end.
