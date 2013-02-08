-module(primary_backup).
-export([
      new/4,
      new_replica/3,
      do/3,
      reconfigure/4,
      stop/4
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

% Create a new primary/backup replicated state machine
new(CoreSettings = {Module, _}, PBArgs, Nodes, Retry) ->
   % spawn new replicas
   Replicas = [ new_replica(N, CoreSettings, PBArgs) || N <- Nodes ],
   % create a configuration and inform all the replicas of it
   Conf0 = #rconf{protocol = ?MODULE, args = {Module, PBArgs}, version = 0},
   reconfigure(Conf0, Replicas, [], Retry).   % returns the new configuration


% Start a new replica
new_replica(Node, CoreSettings, _RepArgs) ->
   server:start(Node, ?MODULE, CoreSettings).

% Send a command to a replicated object
do(#rconf{pids=Replicas=[Primary | Backups], args={C, Args}}, Command, Retry) ->
   Target = case proplists:lookup(read_src, Args) of
      % non-mutating commands go to a random backup
      {_, backup} when Backups /= [] ->
         case C:is_mutating(Command) of
            true -> Primary;
            false -> lists:nth( random:uniform(length(Backups)) , Backups )
         end;

      % non-mutating commands go to a random replica
      {_, random} ->
         case C:is_mutating(Command) of
            true -> Primary;
            false -> lists:nth( random:uniform(length(Replicas)) , Replicas )
         end;

      % either a mutating command, or all commands go to primary
      _ ->
         Primary
   end,
   repobj_utils:call(Target, command, Command, Retry).


% Reconfigure the replicated object with a new set of replicas
reconfigure(OldConf, NewReplicas, NewArgs, Retry) ->
   #rconf{version = Vn, pids = OldReplicas, args = {Module, _}} = OldConf,
   NewConf = OldConf#rconf{
      version = Vn + 1,
      pids = NewReplicas,
      args = {Module, NewArgs}
   },
   % This takes out the replicas in the old configuration but not in the new one
   repobj_utils:multicall(OldReplicas, reconfigure, NewConf, Retry),
   % This integrates the replicas in the new configuration that are not old
   repobj_utils:multicall(NewReplicas, reconfigure, NewConf, Retry),
   NewConf.    % return the new configuration

% Stop one of the replicas of the replicated object.
stop(Obj=#rconf{version = Vn, pids = OldReplicas}, N, Reason, Retry) ->
   Pid = lists:nth(N, OldReplicas),
   repobj_utils:call(Pid, stop, Reason, Retry),
   NewReplicas = lists:delete(Pid, OldReplicas),
   NewConf = Obj#rconf{version = Vn + 1, pids = NewReplicas},
   repobj_utils:multicall(NewReplicas, reconfigure, NewConf, Retry),
   NewConf.


%%%%%%%%%%%%%%%%%%%%%%
% Callback Functions %
%%%%%%%%%%%%%%%%%%%%%%

% Initialize the state of a new replica
init(_Me, {CoreModule, CoreArgs}) ->
   #state{
      core = libdist_sm:new(CoreModule, CoreArgs),
      conf = #rconf{protocol = ?MODULE},
      unstable = ets:new(unstable_commands, [])
   }.


% Handle a queued message
handle_msg(Me, Message, State = #state{
      core = Core,
      conf = Conf,
      role = Role,
      backups = Backups, num_backups = NumBackups,
      unstable = Unstable,
      stable_count = StableCount,
      next_cmd_num = NextCmdNum
   }) ->
   case Message of
      % Handle command as a primary replica
      {Ref, Client, command, Command} when Role == primary ->
         case Core:is_mutating(Command) of
            true ->
               ets:insert(Unstable, {
                     NextCmdNum,
                     NumBackups,
                     Ref,
                     Client,
                     Command
                  }),
               repobj_utils:multicast(Backups, command, {NextCmdNum, Command}),
               {consume, State#state{next_cmd_num = NextCmdNum + 1}};
            false ->
               Client ! {Ref, Core:do(Command)},
               consume
         end;

      % Handle command as a backup replica
      {_Ref, Primary, command, {NextCmdNum, Command}} ->
         Core:do(Command),
         Primary ! {stabilized, StableCount},
         NewCount = StableCount + 1,
         {consume, State#state{stable_count=NewCount, next_cmd_num=NewCount}};

      % Handle query command as a backup replica
      {Ref, Client, command, Command} ->
         Client ! {Ref, Core:do(Command)},
         consume;

      {stabilized, StableCount} ->
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

      % Change this replica's configuration
      {Ref, Client, reconfigure, NewConf=#rconf{pids=[Head | Tail]}} ->
         Client ! {Ref, ok},
         if
            Head == Me ->
               {consume, State#state{
                     conf = NewConf,
                     role = primary,
                     backups = Tail,
                     num_backups = length(Tail)
                  }};
            true ->
               case lists:member(Me, Tail) of
                  true ->
                     {consume, State#state{
                           conf = NewConf,
                           role = backup,
                           backups = [],
                           num_backups = 0
                        }};
                  false ->
                     Core:stop(reconfigure),
                     {stop, reconfigure}
               end
         end;

      {Ref, Client, get_conf} ->
         Client ! {Ref, Conf},
         consume;

      % Stop this replica
      {Ref, Client, stop, Reason} ->
         Client ! {Ref, Core:stop(Reason)},
         {stop, Reason};

      _ ->
         no_match
   end.
