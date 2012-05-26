-module(primary_backup).
-export([
      new_replica/2,
      do/3,
      fork/4,
      reconfigure/3,
      stop/4
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


% Start a new replica
new_replica({CoreModule, CoreArgs}, _RepArgs) ->
   State = #state{
      core = core:new(CoreModule, CoreArgs),
      conf = #conf{protocol = ?MODULE},
      unstable = ets:new(unstable_commands, [])
   },
   loop(State).

% Send a command to a replicated object
do(_Obj=#conf{pids=[Primary | _]}, Command, Retry) ->
   repobj_utils:call(Primary, command, Command, Retry).

% Fork one of the replicas in this replicated object
fork(Obj, N, Node, Args) ->
   Pid = lists:nth(N, Obj#conf.pids),
   repobj_utils:cast(Pid, fork, {Node, Args}).

% Reconfigure the replicated object with a new set of replicas
reconfigure(Obj=#conf{version = Vn, pids = OldReplicas}, NewReplicas, Retry) ->
   NewConf = Obj#conf{ version = Vn + 1, pids = NewReplicas },
   % This takes out the replicas in the old configuration but not in the new one
   repobj_utils:multicall(OldReplicas, reconfigure, NewConf, Retry),
   % This integrates the replicas in the new configuration that are not old
   repobj_utils:multicall(NewReplicas, reconfigure, NewConf, Retry),
   NewConf.    % return the new configuration

% Stop one of the replicas of the replicated object.
stop(Obj=#conf{version = Vn, pids = OldReplicas}, N, Reason, Retry) ->
   Pid = lists:nth(N, OldReplicas),
   repobj_utils:call(Pid, stop, Reason, Retry),
   NewReplicas = lists:delete(Pid, OldReplicas),
   NewConf = Obj#conf{version = Vn + 1, pids = NewReplicas},
   repobj_utils:multicall(NewReplicas, reconfigure, NewConf, Retry),
   NewConf.


%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%

loop(State = #state{
      core = Core,
      conf = Conf,
      role = Role,
      backups = Backups, num_backups = NumBackups,
      unstable = Unstable,
      stable_count = StableCount,
      next_cmd_num = NextCmdNum
   }) ->
   receive
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
               loop(State#state{next_cmd_num = NextCmdNum + 1});
            false ->
               Client ! {Ref, Core:do(Command)},
               loop(State)
         end;

      % Handle command as a backup replica
      {_Ref, Primary, command, {NextCmdNum, Command}} ->
         Core:do(Command),
         Primary ! {stabilized, StableCount},
         NewCount = StableCount + 1,
         loop(State#state{stable_count = NewCount, next_cmd_num = NewCount});

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
         loop(State#state{stable_count = NewStableCount});

      % Fork this replica
      {Ref, Client, fork, {ForkNode, ForkArgs}} ->
         % fork the local core and prepare the local state to be forked
         ForkedState = State#state{
            core = Core:fork(ForkNode, ForkArgs),
            conf = undefined_after_fork,
            role = undefined_after_fork,
            backups = [],
            num_backups = 0
         },
         % serialize the unstable commands (if any)
         UnstableList = ets:tab2list(Unstable),
         % create a forked replica with its own copy of unstable commands
         ForkedPid = spawn(ForkNode, fun() ->
                  ForkedUnstable = ets:new(unstable_commands, []),
                  ets:insert(ForkedUnstable, UnstableList),
                  loop(ForkedState#state{unstable = ForkedUnstable})
            end),
         Client ! {Ref, ForkedPid},
         loop(State);

      % Change this replica's configuration
      {Ref, Client, reconfigure, NewConf=#conf{pids=[Head | Tail]}} ->
         Client ! {Ref, ok},
         if
            Head == self() ->
               loop(State#state{
                     conf = NewConf,
                     role = primary,
                     backups = Tail,
                     num_backups = length(Tail)
                  });
            true ->
               case lists:member(self(), Tail) of
                  true ->
                     loop(State#state{
                           conf = NewConf,
                           role = backup,
                           backups = [],
                           num_backups = 0
                        });
                  false ->
                     Core:stop(reconfiguration)
               end
         end;

      {Ref, Client, get_conf} ->
         Client ! {Ref, Conf},
         loop(State);

      % Stop this replica
      {Ref, Client, stop, Reason} ->
         Client ! {Ref, Core:stop(Reason)};

      % Unexpected message
      UnexpectedMessage ->
         io:format("Received unexpected message ~p at ~p (~p)\n",
            [UnexpectedMessage, self(), ?MODULE])
   end.
