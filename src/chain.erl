-module(chain).
-export([
      new/4,
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
      previous,
      next,
      last,
      unstable,
      stable_count = 0,
      next_cmd_num = 0
   }).

% Create a new chain replicated state machine
new(CoreSettings = {Module, _}, ChainArgs, Nodes, Retry) ->
   % spawn new replicas
   Replicas = [
      spawn(N, ?MODULE, new_replica, [CoreSettings, ChainArgs]) || N <- Nodes ],
   % create a configuration and inform all the replicas of it
   Conf0 = #conf{protocol = ?MODULE, args = Module, version = 0},
   reconfigure(Conf0, Replicas, Retry).   % returns the new configuration

% Start a new replica
new_replica({CoreModule, CoreArgs}, _RepArgs) ->
   State = #state{
      core = core:new(CoreModule, CoreArgs),
      conf = #conf{protocol = ?MODULE},
      unstable = ets:new(unstable_commands, [])
   },
   loop(State).

% Send a command to a replicated object
do(_Obj=#conf{pids = Pids = [Head | _], args = CoreModule}, Command, Retry) ->
   Target = case CoreModule:is_mutating(Command) of
      true -> Head;
      false -> lists:last(Pids)
   end,
   repobj_utils:call(Target, command, Command, Retry).

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
      previous = Prev,
      next = Next,
      last = Last,
      unstable = Unstable,
      stable_count = StableCount,
      next_cmd_num = NextCmdNum
   }) ->
   receive
      % Handle command as the HEAD of the chain
      {Ref, Client, command, Command} = Msg when Prev == chain_head ->
         case Core:is_mutating(Command) of
            true ->
               ets:insert(Unstable, {NextCmdNum, Ref, Client, Command}),
               Next ! {Ref, Client, command, NextCmdNum, Command},
               loop(State#state{next_cmd_num = NextCmdNum + 1});
            false ->
               Last ! Msg,
               loop(State)
         end;

      % Handle command as any replica in the MIDDLE of the chain
      {Ref, Client, command, NextCmdNum, Cmd} = Msg when Next /= chain_tail ->
         Next ! Msg,
         ets:insert(Unstable, {NextCmdNum, Ref, Client, Cmd}),
         loop(State#state{next_cmd_num = NextCmdNum + 1});

      % Handle update command as the TAIL of the chain
      {Ref, Client, command, NextCmdNum, Command} ->
         Client ! {Ref, Core:do(Command)},
         Prev ! {stabilized, NextCmdNum},
         NextCount = NextCmdNum + 1,
         loop(State#state{next_cmd_num = NextCount, stable_count = NextCount});

      % Handle query command as the TAIL of the chain
      {Ref, Client, command, Command} ->
         Client ! {Ref, Core:do(Command)},
         loop(State);

      {stabilized, StableCount} = Msg ->
         case ets:lookup(Unstable, StableCount) of
            [{StableCount, _Ref, _Client, Command}] ->
               Core:do(Command),
               if
                  Prev /= chain_head -> Prev ! Msg;
                  true -> do_not_forward
               end,
               ets:delete(Unstable, StableCount),
               loop(State#state{stable_count = StableCount + 1});

            Other ->
               io:format("Unexpected result when stabilizing at chain replica:
                  ~p\n", [Other]),
               exit(error_stabilizing_at_chain_node)
         end;


      % Fork this replica
      {Ref, Client, fork, {ForkNode, ForkArgs}} ->
         ForkedState = State#state{
            core = Core:fork(ForkNode, ForkArgs),
            conf = undefined_after_fork,
            previous = undefined_after_fork,
            next = undefined_after_fork,
            last = undefined_after_fork
         },
         UnstableList = ets:tab2list(Unstable),
         ForkedPid = spawn(ForkNode, fun() ->
                  ForkedUnstable = ets:new(unstable_commands, []),
                  ets:insert(ForkedUnstable, UnstableList),
                  loop(ForkedState#state{unstable = ForkedUnstable})
            end),
         Client ! {Ref, ForkedPid},
         loop(State);

      % Change this replica's configuration
      {Ref, Client, reconfigure, NewConf=#conf{pids=NewReplicas}} ->
         Client ! {Ref, ok},
         Self = self(),
         case lists:member(Self, NewReplicas) of
            true ->
               {_, NewPrev, NewNext} = repobj_utils:ipn(Self, NewReplicas),
               loop(State#state{
                     conf = NewConf,
                     previous = NewPrev,
                     next = NewNext,
                     last = lists:last(NewReplicas)
                  });
            false ->
               Core:stop(reconfigure)
         end;

      % Return the configuration at this replica
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
