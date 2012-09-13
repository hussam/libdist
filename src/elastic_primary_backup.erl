-module(elastic_primary_backup).
-export([
      activeLoop/5
   ]).

-include("repobj.hrl").

-record(state, {
      self,
      core,
      conf,
      role,
      backups = [],
      num_backups = 0,
      unstable,
      stable_count = 0,
      next_cmd_num = 0
   }).


% State: ACTIVE
% When a replica is in an active state it can add commands to its history and
% respond to client requests. A replica stays in active state until it is wedged
activeLoop(Core, Conf=#conf{pids = Replicas}, Unstable, StableCount, NextCmdNum) ->
   Self = self(),
   [ Head | Tail ] = Replicas,
   {Role, Backups, NumBackups} = case Self of
      Head -> {primary, Tail, length(Tail)};
      _    -> {backup, [], 0}
   end,
   State = #state{
      self = Self,
      core = Core,
      conf = Conf,
      role = Role,
      backups = Backups,
      num_backups = NumBackups,
      unstable = Unstable,
      stable_count = StableCount,
      next_cmd_num = NextCmdNum
   },
   activeLoop(State).


activeLoop(State = #state{
            self = Self,
            core = Core,
            conf = Conf = #conf{version = Vn},
            role = Role,
            backups = Backups, num_backups = NumBackups,
            unstable = Unstable,
            stable_count = StableCount,
            next_cmd_num = NextCmdNum
   }) ->
   receive
      % Transition: add a command to stable history
      {Ref, Client, command, {Vn, Command}} when Role == primary ->
         ets:insert(Unstable, {NextCmdNum, NumBackups, Ref, Client, Command}),
         Msg = {Self, Vn, command, NextCmdNum, Command},
         [ B ! Msg || B <- Backups],
         activeLoop(State#state{next_cmd_num = NextCmdNum + 1});

      {Primary, Vn, command, NextCmdNum, Command} ->
         Core:do(Command),
         Primary ! {Vn, stabilized, NextCmdNum},
         activeLoop(State#state{stable_count=StableCount+1, next_cmd_num=NextCmdNum+1});

      {Vn, stabilized, StableCount} ->
         NewStableCount = case ets:update_counter(Unstable, StableCount, -1) of
            0 ->
               [{_, 0, Ref, Client, Cmd}] = ets:lookup(Unstable, StableCount),
               Client ! {Ref, Core:do(Cmd)},
               ets:delete(Unstable, StableCount),
               StableCount + 1;
            _ ->
               StableCount
         end,
         activeLoop(State#state{stable_count = NewStableCount});


      % Transition: wedgeState
      % take replica into immutable state
      {Ref, Client, wedge, Vn} ->
         Client ! {Ref, wedged},
         elastic:immutableLoop(Core, Conf, Unstable, StableCount, NextCmdNum);

      % create a forked copy of this replica on this node
      {Ref, Client, fork, {Vn, ForkNode, ForkArgs}} ->
         Client ! { Ref,
            elastic:fork(Core, Conf, Unstable, StableCount, NextCmdNum,
               ForkNode, ForkArgs) },
         activeLoop(State);


      % Return current configuration
      {Ref, Client, get_conf} ->
         Client ! {Ref, Conf},
         activeLoop(State);

      % Stop this replica
      {Ref, Client, stop, Reason} ->
         Client ! {Ref, Core:stop(Reason)};


      % ignore other tagged messages and respond with current configuration
      {Ref, Client, _, _} ->
         Client ! {Ref, {error, Conf}},
         activeLoop(State);

      % ignore everything else
      _ ->
         activeLoop(State)
   end.