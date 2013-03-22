-module(primary_backup).
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
      tags = [],
      counter = 0,
      stable_counts = dict:from_list([ {[], 0} ]),
      next_cmd_nums = dict:from_list([ {[], 0} ])
   }).



%%%%%%%%%%%%%%%%%%%%%
% Replica Callbacks %
%%%%%%%%%%%%%%%%%%%%%

% This is a partitioning protocol and it does not require processing of extra arguments
type() -> ?REPL.
conf_args(Args) -> Args.


% Send an asynchronous command to a replicated object
cast(#conf{replicas=Reps=[P | Bs], sm_mod=SMMod, args=Args}, RId, Command) ->
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
   libdist_utils:cast(Target, RId, {Tag, Command}).


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
export(State=#pb_state{unstable=U, stable_counts=SC, next_cmd_nums=NCN}) ->
   State#pb_state{
      unstable = ets:tab2list(U),
      stable_counts = dict:to_list(SC),
      next_cmd_nums = dict:to_list(NCN)
   }.

% Export part of a primary-backup replica's state
export(State = #pb_state{tags = OldTags}, NewTag) ->
   % TODO: implement this properly!!
   export(State#pb_state{tags = [NewTag | OldTags]}).


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


% Handle a queued message
handle_msg(Me, Message, ASE = _AllowSideEffects, SM, State = #pb_state{
      role = Role,
      backups = Backups, num_backups = NumBackups,
      unstable = Unstable,
      tags = Tags,
      counter = Counter,
      stable_counts = StableCounts,
      next_cmd_nums = NextCmdNums
   }) ->
   case Message of
      % Handle command as a primary replica
      {Ref, Client, RId, {write, Command}} when Role == primary ->
         CmdNum = {Tags, Counter},
         ets:insert(Unstable, {
               CmdNum,
               NumBackups,
               Ref,
               Client,
               RId,
               Command
            }),
         Msg = {CmdNum, Me, RId, Ref, Client, Command},
         [ ?SEND(B, RId, Msg, ASE) || B <- Backups ],
         {consume, State#pb_state{counter = Counter + 1}};

      % Handle command as a backup replica
      {CmdNum, Primary, RId, Ref, Client, Cmd} ->
         case libdist_utils:is_next_cmd(CmdNum, NextCmdNums) of
            {true, UpdatedNextCmdNums} ->
               % adding command to unstable + num backups is useful in case of
               % promotion to primary due to failure recovery
               ets:insert(Unstable, {CmdNum, NumBackups, Ref, Client, Cmd}),
               ?SEND(Primary, RId, {ack, CmdNum}, ASE),
               {consume, State#pb_state{next_cmd_nums = UpdatedNextCmdNums}};
            false ->
               keep
         end;

      % Handle stabilizing a write command at a backup replica
      {stabilized, CmdNum} ->
         case libdist_utils:is_next_cmd(CmdNum, StableCounts) of
            {true, NewStableCounts} ->
               [{CmdNum, _, _, _, Command}] = ets:lookup(Unstable, CmdNum),
               ldsm:do(SM, Command, false),
               ets:delete(Unstable, CmdNum),
               {consume, State#pb_state{stable_counts = NewStableCounts}};
            false ->
               keep
         end;


      % Handle query read-only command
      {Ref, Client, _RId, {read, Command}} ->
         ldsm:do(SM, Ref, Client, Command, ASE),
         consume;

      % Handle acknowledgment of receipt of a write command from a backup
      {ack, CmdNum} ->
         case libdist_utils:is_next_cmd(CmdNum, StableCounts) of
            {true, NewStableCounts} ->
               case ets:update_counter(Unstable, CmdNum, -1) of
                  0 ->
                     [{_, 0, Ref, Client, RId, Cmd}] = ets:lookup(Unstable, CmdNum),
                     ldsm:do(SM, Ref, Client, Cmd, ASE),
                     [?SEND(B, RId, {stabilized, CmdNum}, ASE) || B <- Backups],
                     ets:delete(Unstable, CmdNum),
                     {consume, State#pb_state{stable_counts = NewStableCounts}};
                  _ ->
                     {consume, State}
               end;
            false ->
               keep
         end;

      _ ->
         no_match
   end.
