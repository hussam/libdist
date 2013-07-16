-module(elastic_primary_backup).
-export([
      update_state/2,
      handle_msg/6
   ]).

-include("helper_macros.hrl").
-include("libdist.hrl").
-include("elastic.hrl").

update_state(Me, _Conf = #conf{replicas = NewReps}) ->
   [ Head | Tail ] = NewReps,
   case Me of
      Head -> {Tail, length(Tail)};
      _    -> {[], 0}
   end.


handle_msg(Me, Vn, Message, ASE = _AllowSideEffects, SM,
   State = #elastic_state{
      unstable = Unstable,
      stable_count = StableCount,
      next_cmd_num = NextCmdNum,
      pstate = {Backups, NumBackups}
   }) ->
   case Message of
      {Ref, Client, {Vn, {read, Command}}} ->
         ets:insert(Unstable, {NextCmdNum, NumBackups, {read, Ref, Client, Command}}),
         Msg = {Vn, {read, NextCmdNum, Me}},
         [ ?SEND(B, Msg, ASE) || B <- Backups ],
         {consume, State#elastic_state{next_cmd_num = NextCmdNum + 1}};

      {Ref, Client, {Vn, {write, Command}}} ->
         ets:insert(Unstable, {NextCmdNum, NumBackups, {write, Ref, Client, Command}}),
         Msg = {Vn, {write, NextCmdNum, Command, Me}},
         [ ?SEND(B, Msg, ASE) || B <- Backups ],
         {consume, State#elastic_state{next_cmd_num = NextCmdNum + 1}};

      {Vn, ProtocolMessage} ->
         case ProtocolMessage of
            {read, NextCmdNum, Primary} ->
               ?SEND(Primary, {Vn, {ack, NextCmdNum}}, ASE),
               {consume, State#elastic_state{next_cmd_num = NextCmdNum + 1}};

            {write, NextCmdNum, Command, Primary} ->
               ets:insert(Unstable, {NextCmdNum, Command}),
               ?SEND(Primary, {Vn, {ack, NextCmdNum}}, ASE),
               {consume, State#elastic_state{next_cmd_num = NextCmdNum + 1}};

            {stabilized, read, StableCount} ->
               {consume, State#elastic_state{stable_count = StableCount + 1}};

            {stabilized, write, StableCount} ->
               [{StableCount, Cmd}] = ets:lookup(Unstable, StableCount),
               ldsm:do(SM, Cmd, false),
               {consume, State#elastic_state{stable_count = StableCount + 1}};

            {ack, StableCount} ->
               NewStableCount = case ets:update_counter(Unstable, StableCount, -1) of
                  0 ->
                     [{_, 0, {CmdType, Ref, Client, Cmd}}] =
                                             ets:lookup(Unstable, StableCount),
                     ldsm:do(SM, Ref, Client, Cmd, ASE),
                     [?SEND(B, {Vn, {stabilized, CmdType, StableCount}}, ASE) ||
                        B <- Backups],
                     ets:delete(Unstable, StableCount),
                     StableCount + 1;
                  _ ->
                     StableCount
               end,
               {consume, State#elastic_state{stable_count = NewStableCount}}
         end;

      % ignore everything else
      _ ->
         no_match
   end.
