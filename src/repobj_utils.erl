-module(repobj_utils).
-export([
      ipn/2,
      cast/3,
      multicast/3,
      call/4,
      anycall/4,
      multicall/4,
      multicall/5,
      collectMany/3
   ]).

-include("repobj.hrl").

% return the {Index, Previous, Next} elements of a chain member
% the previous of the first chain member is chain_head
% the next of the last chain member is chain_tail
ipn(Pid, Chain) ->
   ipn(Pid, Chain, 1).
ipn(Pid, [Pid, Next | _], 1) -> {1, chain_head, Next};
ipn(Pid, [Prev, Pid, Next | _], Index) -> {Index + 1, Prev, Next};
ipn(Pid, [Prev, Pid], Index) -> {Index + 1, Prev, chain_tail};
ipn(Pid, [_ | Tail], Index) -> ipn(Pid, Tail, Index + 1).


% send an asynchronous request to the given process
% returns the request's reference
cast(Pid, Tag, Request) ->
   Ref = make_ref(),
   Pid ! {Ref, self(), Tag, Request},
   Ref.


% send an asynchronous request to all the processes of a list
% returns the request's reference
multicast(Pids, Tag, Request) ->
   Ref = make_ref(),
   Self = self(),
   % TODO: perhaps spawn separate processes to send in parallel
   [ Pid ! {Ref, Self, Tag, Request} || Pid <- Pids ],
   Ref.


% send synchronous request to a process
call(Pid, Tag, Request, Retry) ->
   call(Pid, make_ref(), Tag, Request, Retry).


% send parallel requests to all processes in a list and wait for one response
anycall(Pids, Tag, Request, Retry) ->
   multicall(Pids, Tag, Request, 1, Retry).


% send parallel requests to all processes in a list and wait for all responses
multicall(Pids, Tag, Request, Retry) ->
   multicall(Pids, Tag, Request, length(Pids), Retry).


% send parallel requests to all processes and wait to get NumResponses responess
multicall(Pids, Tag, Request, NumResponses, Retry) ->
   Parent = self(),
   Ref = make_ref(),
   % create a sub-process for each Pid to make a call
   [spawn(fun()-> Parent ! {Ref, Pid, call(Pid, Ref, Tag, Request, Retry)} end)
      || Pid <- Pids],
   collectMany(Ref, [], NumResponses).


% Collect a required number of responses and return them
collectMany(_Ref, Responses, Required) when length(Responses) == Required ->
   Responses;
collectMany(Ref, Responses, Required) ->
   receive
      {Ref, Pid, Result} ->
         NewResponses = [ {Pid, Result} | lists:keydelete(Pid, 1, Responses) ],
         collectMany(Ref, NewResponses, Required)
   end.



%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%


% send synchronous request to a given Pid
call(Pid, Ref, Tag, Request, RetryAfter) ->
   Pid ! {Ref, self(), Tag, Request},
   receive
      {Ref, Result} -> Result
   after
      RetryAfter -> call(Pid, Ref, Tag, Request, RetryAfter)
   end.

