-module(primary_backup).
-behaviour(replica).

% replica callbacks
-export([
      type/0,
      conf_args/1,
      cast/2,
      init_replica/1,
      import/1,
      export/1,
      update_state/3,
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

% This is a partitioning protocol and it does not require processing of extra arguments
type() -> ?REPL.
conf_args(Args) -> Args.


% Send an asynchronous command to a replicated object
cast(#conf{replicas=Replicas=[P | Bs], sm_mod=SMModule, args=Args}, Command) ->
   {Target, Tag} = case SMModule:is_mutating(Command) of
      true ->
         {P, write};
      false ->
         case proplists:lookup(read_src, Args) of
            % non-mutating commands go to a random backup
            {_, backup} when Bs /= [] ->
               {lists:nth( random:uniform(length(Bs)) , Bs ), read};
            % non-mutating commands go to a random replica
            {_, random} ->
               {lists:nth( random:uniform(length(Replicas)) , Replicas ), read};
            % all commands go to P
            _ ->
               {P, read}
         end
   end,
   libdist_utils:cast(Target, {Tag, Command}).


% Initialize the state of a new replica
init_replica(_Me) ->
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
            num_backups = 0
         }
   end.


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
         Msg = {Ref, Me, write, NextCmdNum, Command},
         [ ?SEND(B, Msg, ASE) || B <- Backups ],
         {consume, State#pb_state{next_cmd_num = NextCmdNum + 1}};

      % Handle command as a backup replica
      {_Ref, Primary, write, NextCmdNum, Command} ->
         ldsm:do(SM, Command, false),
         ?SEND(Primary, {stabilized, StableCount}, ASE),
         NewCount = StableCount + 1,
         {consume, State#pb_state{stable_count=NewCount, next_cmd_num=NewCount}};

      % Handle query read-only command
      {Ref, Client, {read, Command}} ->
         ldsm:do(SM, Ref, Client, Command, ASE),
         consume;

      {stabilized, StableCount} ->
         NewStableCount = case ets:update_counter(Unstable, StableCount, -1) of
            0 ->
               [{_, 0, Ref, Client, Cmd}] = ets:lookup(Unstable, StableCount),
               ldsm:do(SM, Ref, Client, Cmd, ASE),
               ets:delete(Unstable, StableCount),
               StableCount + 1;
            _ ->
               StableCount
         end,
         {consume, State#pb_state{stable_count = NewStableCount}};

      _ ->
         no_match
   end.
