-module(primary_backup).
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

-record(pb_state, {
      role,
      backups = [],
      num_backups = 0,
      unstable,
      stable_count = 0,
      next_cmd_num = 0
   }).



%%%%%%%%%%%%%%%%%%%%%
% Replica Callbacks %
%%%%%%%%%%%%%%%%%%%%%

% This is a replication protocol and it does not require processing of extra
% arguments and it does not overload any generic repobj functions
type() -> ?REPL.
conf_args(Args) -> Args.
overloads(_) -> false.


% Send an asynchronous command to a replicated object
cast(#conf{replicas=Reps=[P | Bs], sm_mod=SMMod, args=Args}, Command) ->
   {Target, Tag} = case SMMod:is_mutating(Command) of
      true ->
         {P, write};
      false ->
         case proplists:lookup(read_src, Args) of
            % non-mutating commands go to a random backup
            {_, backup} when Bs /= [] ->
               {lists:nth( random:uniform(length(Bs)) , Bs ), read};
            % non-mutating commands go to a random replica
            {_, random} ->
               {lists:nth( random:uniform(length(Reps)) , Reps ), read};
            % all commands go to P
            _ ->
               {P, read}
         end
   end,
   libdist_utils:cast(Target, {Tag, Command}).


% Initialize the state of a new replica
init_replica(_Me, _Args) ->
   #pb_state{
      unstable = ets:new(unstable_commands, [])
   }.


% Import a previously exported primary-backup state
import(ExportedState = #pb_state{unstable = UnstableList}) ->
   Unstable = ets:new(unstable_commands, []),
   ets:insert(Unstable, UnstableList),
   ExportedState#pb_state{
      unstable = Unstable
   }.


% Export a primary-backup replica state
export(State = #pb_state{unstable = Unstable}) ->
   State#pb_state{
      unstable = ets:tab2list(Unstable)
   }.


% Update the protocol's custom state (due to replacement or reconfiguration)
update_state(Me, #conf{replicas = [Head | Tail]}, State) ->
   case Head == Me of
      true ->
         State#pb_state{
            role = primary,
            backups = Tail,
            num_backups = length(Tail)
         };
      false ->
         State#pb_state{
            role = backup,
            backups = [],
            num_backups = length(Tail) - 1
         }
   end.



% Handle the failure of a replica
handle_failure(Me, Conf = #conf{replicas = Pids}, State, FailedPid, _Info) ->
   NewConf = Conf#conf{
      replicas = lists:delete(FailedPid, Pids)
   },
   {NewConf, update_state(Me, NewConf, State)}.


% Handle a queued message
handle_msg(Me, Message, ASE = _AllowSideEffects, SM, State = #pb_state{
      role = Role,
      backups = Backups, num_backups = NumBackups,
      unstable = Unstable,
      stable_count = StableCount,
      next_cmd_num = NextCmdNum
   }) ->
   case Message of
      % Handle command as a primary replica
      {Ref, Client, {write, Command}} when Role == primary ->
         ets:insert(Unstable, {
               NextCmdNum,
               NumBackups,
               Ref,
               Client,
               Command
            }),
         Msg = {NextCmdNum, Me, Ref, Client, Command},
         [ ?SEND(B, Msg, ASE) || B <- Backups ],
         {consume, State#pb_state{next_cmd_num = NextCmdNum + 1}};

      % Handle command as a backup replica
      {NextCmdNum, Primary, Ref, Client, Cmd} ->
         % adding command to unstable is useful in case of
         % promotion to primary due to failure recovery
         ldsm:do(SM, Cmd, false),
         ets:insert(Unstable, {NextCmdNum, NumBackups, Ref, Client, Cmd}),
         ?SEND(Primary, {ack, NextCmdNum}, ASE),
         {consume, State#pb_state{next_cmd_num = NextCmdNum + 1}};

      % Handle stabilizing a write command at a backup replica
      {stabilized, StableCount} ->
         ets:delete(Unstable, StableCount),
         {consume, State#pb_state{stable_count = StableCount + 1}};

      % Handle query read-only command
      {Ref, Client, {read, Command}} ->
         ldsm:do(SM, Ref, Client, Command, ASE),
         consume;

      % Handle acknowledgment of receipt of a write command from a backup
      {ack, StableCount} ->
         case ets:update_counter(Unstable, StableCount, -1) of
            0 ->
               [{_, 0, Ref, Client, Cmd}] = ets:lookup(Unstable, StableCount),
               ldsm:do(SM, Ref, Client, Cmd, ASE),
               [?SEND(B, {stabilized, StableCount}, ASE) || B <- Backups],
               ets:delete(Unstable, StableCount),
               {consume, State#pb_state{stable_count = StableCount + 1}};
            _ ->
               {consume, State}
         end;

      _ ->
         no_match
   end.
