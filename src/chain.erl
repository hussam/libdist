-module(chain).
-export([
      new/4,
      new_replica/2,
      do/3,
      reconfigure/4,
      stop/4
   ]).

-include("repobj.hrl").

-record(state, {
      core,
      conf,
      previous,
      next,
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
   Conf0 = #rconf{protocol = ?MODULE, args = Module, version = 0},
   reconfigure(Conf0, Replicas, [], Retry).   % returns the new configuration

% Start a new replica
new_replica({CoreModule, CoreArgs}, _RepArgs) ->
   State = #state{
      core = sm:new(CoreModule, CoreArgs),
      conf = #rconf{protocol = ?MODULE},
      unstable = ets:new(unstable_commands, [])
   },
   loop(State).

% Send a command to a replicated object
do(_Obj=#rconf{pids = Pids = [Head | _], args = CoreModule}, Command, Retry) ->
   Target = case CoreModule:is_mutating(Command) of
      true -> Head;
      false -> lists:last(Pids)
   end,
   repobj_utils:call(Target, command, Command, Retry).

% Reconfigure the replicated object with a new set of replicas
reconfigure(OldConf, NewReplicas, _NewArgs, Retry) ->
   #rconf{version = Vn, pids = OldReplicas} = OldConf,
   NewConf = OldConf#rconf{ version = Vn + 1, pids = NewReplicas },
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


%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%

loop(State = #state{
      core = Core,
      conf = Conf,
      previous = Prev,
      next = Next,
      unstable = Unstable,
      stable_count = StableCount,
      next_cmd_num = NextCmdNum
   }) ->
   receive
      % Handle command as the HEAD of the chain
      {Ref, Client, command, Command} when Prev == chain_head ->
         ets:insert(Unstable, {NextCmdNum, Ref, Client, Command}),
         Next ! {Ref, Client, command, NextCmdNum, Command},
         loop(State#state{next_cmd_num = NextCmdNum + 1});

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

      % Change this replica's configuration
      {Ref, Client, reconfigure, NewConf=#rconf{pids=NewReplicas}} ->
         Client ! {Ref, ok},
         Self = self(),
         case lists:member(Self, NewReplicas) of
            true ->
               {_, NewPrev, NewNext} = repobj_utils:ipn(Self, NewReplicas),
               loop(State#state{
                     conf = NewConf,
                     previous = NewPrev,
                     next = NewNext
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
