-module(chain).
-compile({inline, [handle_msg/4]}).

% Repobj interface
-export([
      new/4,
      new_replica/3,
      cast/2,
      call/3,
      reconfigure/4,
      stop/4
   ]).

% State machine interface
-export([
      new/1,
      handle_cmd/3,
      is_mutating/1,
      stop/2
   ]).

% Server callbacks
-export([
      init/2,
      handle_msg/3
   ]).

-include("helper_macros.hrl").
-include("libdist.hrl").

-record(state, {
      me,
      core,
      conf,
      previous,
      next,
      unstable,
      stable_count = 0,
      next_cmd_num = 0
   }).

% Create a new chain replicated state machine
new(CoreSettings = {Module, _}, ChainArgs, Nodes, Timeout) ->
   % spawn new replicas
   Replicas = [ new_replica(N, CoreSettings, ChainArgs) || N <- Nodes ],
   % create a configuration and inform all the replicas of it
   Conf0 = #rconf{protocol = ?MODULE, args = Module, version = 0},
   reconfigure(Conf0, Replicas, [], Timeout).   % return includes configuration


% Start a new replica
new_replica(Node, CoreSettings, _RepArgs) ->
   server:start(Node, ?MODULE, CoreSettings).


% Send an asynchronous command to a replicated object
cast(_Obj=#rconf{pids = Pids = [Head | _], args = CoreModule}, Command) ->
   Target = case CoreModule:is_mutating(Command) of
      true -> Head;
      false -> lists:last(Pids)
   end,
   libdist_utils:cast(Target, command, Command).


% Send a synchronous command to a replicated object
call(Obj, Command, Timeout) ->
   libdist_utils:collect(cast(Obj, Command), Timeout).


% Reconfigure the replicated object with a new set of replicas
reconfigure(OldConf, NewReplicas, _NewArgs, Timeout) ->
   #rconf{version = Vn, pids = OldReplicas} = OldConf,
   NewConf = OldConf#rconf{ version = Vn + 1, pids = NewReplicas },
   % This takes out the replicas in the old configuration but not in the new one
   Refs = libdist_utils:multicast(OldReplicas, reconfigure, NewConf),
   case libdist_utils:collectall(Refs, Timeout) of
      {ok, _} ->
         % This integrates replicas in the new configuration that are not old
         Refs2 = libdist_utils:multicast(NewReplicas, reconfigure, NewConf),
         case libdist_utils:collectall(Refs2, Timeout) of
            {ok, _} ->
               {ok, NewConf};
            Error ->
               Error
         end;
      % TODO: distinguish which case of timeout occurred
      Error ->
         Error
   end.


% Stop one of the replicas of the replicated object.
stop(Obj=#rconf{version = Vn, pids = OldReplicas}, N, Reason, Timeout) ->
   Pid = lists:nth(N, OldReplicas),
   case libdist_utils:collect(libdist_utils:cast(Pid, stop, Reason), Timeout) of
      {ok, _} ->
         NewReplicas = lists:delete(Pid, OldReplicas),
         NewConf = Obj#rconf{version = Vn + 1, pids = NewReplicas},
         Refs = libdist_utils:multicast(NewReplicas, reconfigure, NewConf),
         case libdist_utils:collectall(Refs, Timeout) of
            {ok, _} ->
               {ok, NewConf};
            Error ->
               Error
         end;
      % TODO: distinguish which case of timeout occurred
      Error ->
         Error
   end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%
% State Machine Callbacks %
%%%%%%%%%%%%%%%%%%%%%%%%%%%


new({CoreModule, CoreArgs}) ->
   init([], {CoreModule, CoreArgs}).

handle_cmd(State = #state{me = Me}, AllowSideEffects, Message) ->
   handle_msg(Me, Message, AllowSideEffects, State).

is_mutating(_) ->
   true.

stop(#state{core = Core}, Reason) ->
   Core:stop(Reason).


%%%%%%%%%%%%%%%%%%%%
% Server Callbacks %
%%%%%%%%%%%%%%%%%%%%


% Initialize the state of a new replica
init(Me, {CoreModule, CoreArgs}) ->
   #state{
      me = Me,
      core = libdist_sm:new(CoreModule, CoreArgs),
      conf = #rconf{protocol = ?MODULE},
      unstable = ets:new(unstable_commands, [])
   }.


% Handle a queued message
handle_msg(Me, Message, State) ->
   handle_msg(Me, Message, true, State).


% Handle a queued message
handle_msg(Me, Message, AllowSideEffects, State = #state{
      core = Core,
      conf = Conf,
      previous = Prev,
      next = Next,
      unstable = Unstable,
      stable_count = StableCount,
      next_cmd_num = NextCmdNum
   }) ->
   case Message of
      % Handle command as the HEAD of the chain
      {Ref, Client, command, Command} when Prev == chain_head ->
         ets:insert(Unstable, {NextCmdNum, Ref, Client, Command}),
         Next ! {Ref, Client, command, NextCmdNum, Command},
         {consume, State#state{next_cmd_num = NextCmdNum + 1}};

      % Handle command as any replica in the MIDDLE of the chain
      {Ref, Client, command, NextCmdNum, Cmd} = Msg when Next /= chain_tail ->
         Next ! Msg,
         ets:insert(Unstable, {NextCmdNum, Ref, Client, Cmd}),
         {consume, State#state{next_cmd_num = NextCmdNum + 1}};

      % Handle update command as the TAIL of the chain
      {Ref, Client, command, NextCmdNum, Command} ->
         ?ECS({Ref, Core:do(AllowSideEffects, Command)}, AllowSideEffects, Client),
         Prev ! {stabilized, NextCmdNum},
         NextCount = NextCmdNum + 1,
         {consume, State#state{next_cmd_num=NextCount, stable_count=NextCount}};

      % Handle query command as the TAIL of the chain
      {Ref, Client, command, Command} ->
         ?ECS({Ref, Core:do(AllowSideEffects, Command)}, AllowSideEffects, Client),
         consume;

      {stabilized, StableCount} = Msg ->
         case ets:lookup(Unstable, StableCount) of
            [{StableCount, _Ref, _Client, Command}] ->
               Core:do(AllowSideEffects, Command),
               if
                  Prev /= chain_head -> Prev ! Msg;
                  true -> do_not_forward
               end,
               ets:delete(Unstable, StableCount),
               {consume, State#state{stable_count = StableCount + 1}};

            Other ->
               io:format("Unexpected result when stabilizing at chain replica:
                  ~p\n", [Other]),
               exit(error_stabilizing_at_chain_node)
         end;

      % Change this replica's configuration
      % TODO: handle reconfiguration in nested protocols
      {Ref, Client, reconfigure, NewConf=#rconf{pids=NewReplicas}} ->
         ?ECS({Ref, ok}, AllowSideEffects, Client),
         case lists:member(Me, NewReplicas) of
            true ->
               {_, NewPrev, NewNext} = libdist_utils:ipn(Me, NewReplicas),
               {consume, State#state{
                     conf = NewConf,
                     previous = NewPrev,
                     next = NewNext
                  }};
            false ->
               Core:stop(reconfigure),
               {stop, reconfigure}
         end;

      % Return the configuration at this replica
      {Ref, Client, get_conf} ->
         ?ECS({Ref, Conf}, AllowSideEffects, Client),
         consume;

      % Stop this replica
      {Ref, Client, stop, Reason} ->
         ?ECS({Ref, Core:stop(Reason)}, AllowSideEffects, Client),
         {stop, Reason};

      _ ->
         no_match
   end.
