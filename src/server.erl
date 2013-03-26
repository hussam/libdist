-module(server).

-export([
      start/3
   ]).


% Start a server on the given node and register a handler module
start(Node, HandlerModule, InitArgs) ->
   spawn(Node, fun() -> recv_loop(HandlerModule, InitArgs) end).


%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%


% Receive Loop: buffer incoming messages until the worker process can take them.
recv_loop(HandlerModule, InitArgs) ->
   Self = self(),
   WorkerPID = spawn(fun() -> work_loop(Self, HandlerModule, InitArgs) end),
   recv_loop([], 0, WorkerPID, true).

recv_loop(MsgStack, StackLen, WorkerPID, WorkerFree) ->
   receive
      '$worker_done' when StackLen > 0 ->
         WorkerPID ! {'$new_msgs', lists:reverse(MsgStack)},
         recv_loop([], 0, WorkerPID, false);

      '$worker_done' ->
         recv_loop([], 0, WorkerPID, true);

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
work_loop(RecvPID, HandlerModule, InitArgs) ->
   State = HandlerModule:init(RecvPID, InitArgs),
   work_loop(RecvPID, HandlerModule, State, []).

work_loop(RecvPID, Handler, State, MessageQ) ->
   {KeptMsgs, NewState} = handle_messages(MessageQ, [], RecvPID, Handler, State),

   RecvPID ! '$worker_done',

   receive
      {'$new_msgs', NewMsgs} ->
         NextQueue = merge_into_queue(KeptMsgs, NewMsgs),
         work_loop(RecvPID, Handler, NewState, NextQueue)
   end.


%%%%%%%%%%%%%%%%%%%%%
% Utility Functions %
%%%%%%%%%%%%%%%%%%%%%


% Process as many messages as possible using the given 'Handler' module. Return
% a list of 'KeptMsgs' that were not consumed by the handler and the new
% state. The list is returned in reverse queue order.
handle_messages([], KeptMsgs, _Addr, _Handler, State) ->
   {KeptMsgs, State};
handle_messages([Message | RemMsgs], KeptMsgs, Addr, Handler, State) ->
   case Handler:handle_msg(Addr, Message, State) of
      % Consume message and modify local state
      {consume, NewState} ->
         handle_messages(RemMsgs, KeptMsgs, Addr, Handler, NewState);

      % Consume message without modifying local state
      consume ->
         handle_messages(RemMsgs, KeptMsgs, Addr, Handler, State);

      % Keep message and modify local state
      {keep, NewState} ->
         handle_messages(RemMsgs, [Message|KeptMsgs], Addr, Handler, NewState);

      % Stop server for the given Reason
      {stop, Reason} ->
         Addr ! {'$stop', Reason},
         exit(Reason);

      % Either 'Keep with no state modification' or 'No Match'
      _ ->
         handle_messages(RemMsgs, [Message|KeptMsgs], Addr, Handler, State)
   end.


% Merge a list into head of an existing message queue. This assumes the list
% elements are in reverse order of how it should appear in the queue.
merge_into_queue([], Queue) ->
   Queue;
merge_into_queue([Head | Tail], Queue) ->
   merge_into_queue(Tail, [Head | Queue]).
