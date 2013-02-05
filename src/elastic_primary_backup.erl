-module(elastic_primary_backup).
-export([
      activate/6
   ]).

% Server callbacks
-export([
      init/2,
      handle_msg/3
   ]).


-include("repobj.hrl").

-record(state, {
      core,
      conf,
      role,
      backups = [],
      num_backups = 0,
      unstable,
      stable_count = 0,
      next_cmd_num = 0
   }).


% Set/return the state of an active/non-immutable replica
activate(Me, Core, Conf=#rconf{pids = Replicas}, Unstable, StableCount, NextCmdNum) ->
   [ Head | Tail ] = Replicas,
   {Role, Backups, NumBackups} = case Me of
      Head -> {primary, Tail, length(Tail)};
      _    -> {backup, [], 0}
   end,
   #state{
      core = Core,
      conf = Conf,
      role = Role,
      backups = Backups,
      num_backups = NumBackups,
      unstable = Unstable,
      stable_count = StableCount,
      next_cmd_num = NextCmdNum
   }.


% Server callback should do nothing since the state should be initiated via the
% activate/6 method.
init(_, _) ->
   not_activated.


% State: ACTIVE
% When a replica is in an active state it can add commands to its history and
% respond to client requests. A replica stays in active state until it is wedged
handle_msg(Me, Message, State = #state{
            core = Core,
            conf = Conf = #rconf{version = Vn},
            role = Role,
            backups = Backups, num_backups = NumBackups,
            unstable = Unstable,
            stable_count = StableCount,
            next_cmd_num = NextCmdNum
   }) ->
   case Message of
      {Ref, Client, read, {Vn, Command}} when Role == primary ->
         Client ! {Ref, Core:do(Command)},
         consume;

      {_, _, read, _} ->
         consume;

      % Transition: add a command to stable history
      {Ref, Client, write, {Vn, Command}} when Role == primary ->
         ets:insert(Unstable, {NextCmdNum, NumBackups, Ref, Client, Command}),
         Msg = {Me, Vn, write, NextCmdNum, Command},
         [ B ! Msg || B <- Backups],
         {consume, State#state{next_cmd_num = NextCmdNum + 1}};

      {Primary, Vn, write, NextCmdNum, Command} ->
         Primary ! {Vn, stabilized, NextCmdNum},
         Core:do(Command),
         {consume, State#state{stable_count=StableCount+1,
               next_cmd_num=NextCmdNum+1}};

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
         {consume, State#state{stable_count = NewStableCount}};


      % Transition: wedgeState
      % take replica into immutable state
      {Ref, Client, wedge, Vn} ->
         Client ! {Ref, wedged},
         {consume, elastic:makeImmutable(Core, Conf, Unstable, StableCount,
               NextCmdNum)};


      % Return current configuration
      {Ref, Client, get_conf} ->
         Client ! {Ref, Conf},
         consume;

      % Stop this replica
      {Ref, Client, stop, Reason} ->
         Client ! {Ref, Core:stop(Reason)},
         {stop, Reason};


      % ignore other tagged messages and respond with current configuration
      {Ref, Client, _, _} ->
         Client ! {Ref, {error, {reconfigured, Conf}}},
         consume;

      % ignore everything else
      _ ->
         consume
   end;

handle_msg(_, _, not_activated) ->
   no_match.
