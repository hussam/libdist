-module(quorum).
-behaviour(replica).

% Replica callbacks
-export([
      type/0,
      conf_args/1,
      cast/2,
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

-record(quorum_state, {
      n,
      r,
      w,
      others = [],
      unstable,
      updates_count = 0
   }).


%%%%%%%%%%%%%%%%%%%%%
% Replica Callbacks %
%%%%%%%%%%%%%%%%%%%%%


% This is a partitioning protocol and it does not require processing of extra arguments
type() -> ?REPL.
conf_args(Args) -> Args.


% Send an asynchronous command to a replicated object
cast(#conf{replicas=Reps=[Hd | _], sm_mod=SMMod, args=QArgs}, Command) ->
   Target = case proplists:get_bool(shuffle, QArgs) of
      true -> lists:nth(random:uniform(length(Reps)), Reps);
      false -> Hd
   end,
   QName = case SMMod:is_mutating(Command) of
      true -> write;
      false -> read
   end,
   libdist_utils:cast(Target, {QName, Command}).


% Initialize the state of a new replica
init_replica(_Me) ->
   #quorum_state{
      unstable = ets:new(unstable_commands, [])
   }.


% Import a previously exported quorum state
import(ExportedState = #quorum_state{unstable = UnstableList}) ->
   Unstable = ets:new(unstable_commands, []),
   ets:insert(Unstable, UnstableList),
   ExportedState#quorum_state{
      unstable = Unstable
   }.


% Export a quorum replica state
export(State = #quorum_state{unstable = Unstable}) ->
   State#quorum_state{
      unstable = ets:tab2list(Unstable)
   }.


% Export part of a chain replica's state
export(State, _Tag) ->
   % TODO: filter unstable requests that are selected by 'Tag'
   export(State).


% Update the protocol's custom state (due to replacement or reconfiguration)
update_state(Me, #conf{replicas = NewReps, args = QArgs}, State) ->
   N = length(NewReps),
   R = case proplists:lookup(r, QArgs) of
      {r, ReadQuorumSize} -> ReadQuorumSize;
      none -> trunc(N/2) + 1
   end,
   W = case proplists:lookup(w, QArgs) of
      {w, WriteQuorumSize} -> WriteQuorumSize;
      none -> trunc(N/2) + 1
   end,
   State#quorum_state{
      n = N,
      r = R,
      w = W,
      others = shuffle(lists:delete(Me, NewReps))
   }.


% Handle failure of a replica
handle_failure(_Me, _NewConf, State, _FailedPid, _Info) ->
   % Do not modify the state since this protocol masks failure.
   % However, coordinator will still send messages to failed replicas according
   % to its old state.
   State.


% Handle a queued message
handle_msg(Me, Message, ASE = _AllowSideEffects, SM, State = #quorum_state{
      n = N,
      r = R,
      w = W,
      others = Others,
      unstable = Unstable,
      updates_count = UpdatesCount
   }) ->
   case Message of
      % Respond to a command as a member of a read quorum
      {Ref, Coordinator, read, Command} ->
         Result = ldsm:do(SM, Command, false),
         ?SEND(Coordinator, {stabilized, Ref, UpdatesCount, Result}, ASE),
         consume;

      % Respond to a command as a member of a write quorum
      {Ref, Coordinator, write, Command} ->
         NewCount = UpdatesCount + 1,
         Result = ldsm:do(SM, Command, false),
         ?SEND(Coordinator, {stabilized, Ref, NewCount, Result}, ASE),
         {consume, State#quorum_state{updates_count = NewCount}};

      {stabilized, Ref, Count, Result} ->
         [{Ref, _, _, Client, Cmd, CurCount, CurResult}] =
                                                      ets:lookup(Unstable, Ref),
         % Keep track of the result reflecting the most number of updates
         {MaxCount, MaxResult} = case Count > CurCount of
            true ->
               ets:update_element(Unstable, Ref, [{6, Count}, {7, Result}]),
               {Count, Result};
            false ->
               {CurCount, CurResult}
         end,

         % count response towards a quorum result by decrementing quorum count
         Return = case ets:update_counter(Unstable, Ref, {2, -1}) of
            0 ->  % quorum reached
               % perform the command locally and reply with the max-count result
               MyResult = ldsm:do(SM, Cmd, ASE),
               MyCount = UpdatesCount + 1,
               FinalResult = case MyCount > MaxCount of
                  true -> MyResult;
                  false -> MaxResult
               end,
               ?SEND(Client, {Ref, FinalResult}, ASE),
               {consume, State#quorum_state{updates_count = MyCount}};
            _ -> % quorum not reached
               consume
         end,
         % decrement total number of possible remaining responses (and clean up)
         case ets:update_counter(Unstable, Ref, {3, -1}) of
            0 -> ets:delete(Unstable, Ref);
            _ -> do_nothing
         end,
         Return;

      % Respond to a client command as a coordinator
      {Ref, Client, {QTag, Command}} ->
         {NextCount, QSize} = case QTag of
            read -> {UpdatesCount, R};
            write -> {UpdatesCount + 1, W}
         end,
         case QSize of
            1 ->
               ldsm:do(SM, Ref, Client, Command, ASE);
            _ ->
               ets:insert(Unstable,
                  {Ref, QSize-1, N-1, Client, Command, -1, []}),
               Msg = {Ref, Me, QTag, Command},
               [ ?SEND(Replica, Msg, ASE) || Replica <- Others ]
         end,
         {consume, State#quorum_state{updates_count = NextCount}};


      _ ->
         no_match
   end.

%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%


% Shuffle a list
shuffle(List) ->
   %% Determine the log n portion then randomize the list.
   randomize(round(math:log(length(List)) + 0.5), List).

randomize(1, List) ->
   randomize(List);
randomize(T, List) ->
   lists:foldl(
      fun(_E, Acc) -> randomize(Acc) end,
      randomize(List),
      lists:seq(1, T - 1)
   ).

randomize(List) ->
   D = lists:map(fun(A) -> {random:uniform(), A} end, List),
   {_, D1} = lists:unzip(lists:keysort(1, D)),
   D1.
