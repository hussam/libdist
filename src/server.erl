-module(server).

-export([
      start/1,
      start/3,
      append_handler/3,
      prepend_handler/3,
      remove_handler/2
   ]).


% Start a server on the given node with no registered handler module
start(Node) ->
   spawn(Node, fun() -> recv_loop() end).

% Start a server on the given node and register a handler module
start(Node, HandlerModule, InitArgs) ->
   PID = start(Node),
   prepend_handler(PID, HandlerModule, InitArgs),
   PID.

% Register a new message handler module at the end of the handlers list
append_handler(RecvPID, HandlerModule, InitArgs) ->
   RecvPID ! {'$append_handler', self(), HandlerModule, InitArgs},
   receive
      ok -> ok
   end.

% Register a new message handler module at the front of the handlers list
prepend_handler(RecvPID, HandlerModule, InitArgs) ->
   RecvPID ! {'$prepend_handler', self(), HandlerModule, InitArgs},
   receive
      ok -> ok
   end.

% Remove a handler module
remove_handler(RecvPID, HandlerModule) ->
   RecvPID ! {'$rm_handler', self(), HandlerModule},
   receive
      ok -> ok
   end.


%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%


% Receive Loop: buffer incoming messages until the worker process can take them.
recv_loop() ->
   Self = self(),
   WorkerPID = spawn(fun() -> work_loop(Self) end),
   recv_loop([], 0, WorkerPID, true).

recv_loop(MsgStack, StackLen, WorkerPID, WorkerFree) ->
   receive
      '$worker_done' when StackLen > 0 ->
         WorkerPID ! {'$new_msgs', lists:reverse(MsgStack)},
         recv_loop([], 0, WorkerPID, false);

      '$worker_done' ->
         recv_loop([], 0, WorkerPID, true);

      {'$append_handler', _Client, _HandlerModule, _InitArgs} = Msg ->
         WorkerPID ! Msg,
         recv_loop(MsgStack, StackLen, WorkerPID, WorkerFree);

      {'$prepend_handler', _Client, _HandlerModule, _InitArgs} = Msg ->
         WorkerPID ! Msg,
         recv_loop(MsgStack, StackLen, WorkerPID, WorkerFree);

      {'$rm_handler', _Client, _HandlerModule} = Msg ->
         WorkerPID ! Msg,
         recv_loop(MsgStack, StackLen, WorkerPID, WorkerFree);

      {'$stop', Reason} ->
         exit(Reason);

      Msg when WorkerFree ->
         WorkerPID ! {'$new_msgs', [Msg]},
         recv_loop([], 0, WorkerPID, false);

      Msg ->
         recv_loop([Msg | MsgStack], StackLen + 1, WorkerPID, false)
   end.


% Worker Loop: processes as many existing messages as possible, then waits
% to receive a batch of new messages from the receive loop. The new messages are
% appended to the list of kept/unprocessed messages.
work_loop(RecvPID) ->
   put('$addr', RecvPID),
   work_loop(RecvPID, [], []).

work_loop(RecvPID, HandlerModules, MessageQueue) ->
   KeptMsgs = handle_messages([], MessageQueue, HandlerModules),

   RecvPID ! '$worker_done',

   receive
      {'$new_msgs', NewMsgs} ->
         NextQueue = merge_into_queue(KeptMsgs, NewMsgs),
         work_loop(RecvPID, HandlerModules, NextQueue);

      {'$append_handler', Client, NewModule, InitArgs} ->
         NextHandlers = HandlerModules ++ [NewModule],
         put(NewModule, NewModule:init(RecvPID, InitArgs)), % initialize handler
         Client ! ok,
         work_loop(RecvPID, NextHandlers, MessageQueue);

      {'$prepend_handler', Client, NewModule, InitArgs} ->
         NextHandlers = [NewModule | HandlerModules],
         put(NewModule, NewModule:init(RecvPID, InitArgs)), % initialize handler
         Client ! ok,
         work_loop(RecvPID, NextHandlers, MessageQueue);

      {'$rm_handler', Client, Module} ->
         NextHandlers = lists:delete(Module, HandlerModules),
         erase(Module),    % XXX: anything else required for state cleanup?
         Client ! ok,
         work_loop(RecvPID, NextHandlers, MessageQueue)
   end.


%%%%%%%%%%%%%%%%%%%%%
% Utility Functions %
%%%%%%%%%%%%%%%%%%%%%


% Process as many messages as possible using the given HandlerModules. Return a
% list of 'KeptMessages' that were not consumed by handlers. The list is
% returned in reverse queue order.
handle_messages(KeptMessages, [], _HandlerModules) ->
   KeptMessages;
handle_messages(KeptMessages, [Message | RemMessages], HandlerModules) ->
   case handle_message(Message, HandlerModules) of
      consume ->
         handle_messages(KeptMessages, RemMessages, HandlerModules);
      keep ->
         handle_messages([Message | KeptMessages], RemMessages, HandlerModules)
   end.


% Process a single message using the given handler modules. Each module's
% handler is tried in order until the message is either 'consumed' or no more
% modules remain. Returns the fate of the message: either 'consume' or 'keep'.
handle_message(_, []) ->
   keep;
handle_message(Message, [Module | RemModules]) ->
   Addr = get('$addr'), State = get(Module),    % XXX: why not send as args?
   case Module:handle_msg(Addr, Message, State) of
      % Consume message and modify local state
      {consume, NewState} ->
         put(Module, NewState),
         consume;

      % Consume message without modifying local state
      consume ->
         consume;

      % Keep message and modify local state
      {keep, NewState} ->
         put(Module, NewState),
         handle_message(Message, RemModules);

      % Stop server for the given Reason
      {stop, Reason} ->
         Addr ! {'$stop', Reason},
         exit(Reason);

      % Either 'Keep with no state modification' or 'No Match'
      _ ->
         handle_message(Message, RemModules)
   end.


% Merge a list into head of an existing message queue. This assumes the list
% elements are in reverse order of how it should appear in the queue.
merge_into_queue([], Queue) ->
   Queue;
merge_into_queue([Head | Tail], Queue) ->
   merge_into_queue(Tail, [Head | Queue]).
