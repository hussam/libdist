-module(libdist_utils).

% Async comm
-export([
      send/3,
      cast/2,
      cast/3,
      collect/2,
      multicast/2,
      collectany/2,
      collectmany/3,
      collectall/2
   ]).

% Sync comm
-export([
      call/3,
      call/4,
      anycall/3,
      multicall/3,
      multicall/4
   ]).

% General utility
-export([
      ipn/2,
      list_replace/3,
      is_next_cmd/2
   ]).

-include("constants.hrl").
-include("libdist.hrl").



%%%%%%%%%%%%%%%%%%%
% General Utility %
%%%%%%%%%%%%%%%%%%%


% return the {Index, Previous, Next} elements of a chain member
% the previous of the first chain member is chain_head
% the next of the last chain member is chain_tail
ipn(Pid, Chain) ->
   ipn(Pid, Chain, 1).
ipn(Pid, [Pid, Next | _], 1) -> {1, chain_head, Next};
ipn(Pid, [Prev, Pid, Next | _], Index) -> {Index + 1, Prev, Next};
ipn(Pid, [Prev, Pid], Index) -> {Index + 1, Prev, chain_tail};
ipn(Pid, [_ | Tail], Index) -> ipn(Pid, Tail, Index + 1).


% replace OldElem with NewElem in List
list_replace(OldElem, NewElem, List) ->
   case lists:splitwith(fun(I) -> I /= OldElem end, List) of
      {Preds, [OldElem | Succs]} ->
         Preds ++ [NewElem | Succs];

      _ ->  % OldElem not in List
         List
   end.


% checks whether the given tagged command number is what is expected for the tag
is_next_cmd({Tags, Num}, CmdNums) ->
   case dict:find(Tags, CmdNums) of
      {ok, Num} ->            % expecting this number with these tags
         {true, dict:store(Tags, Num+1, CmdNums)};
      error ->                % first command from newly partitioned replica
         {true, dict:store(Tags, Num+1, CmdNums)};
      {ok, _} ->              % tags found, but incorrect command number
         false
   end.

%%%%%%%%%%%%%%%%%%%%%%%
% Async Communication %
%%%%%%%%%%%%%%%%%%%%%%%



% send a message directly to a process, or a configuration
% Used for INTRA-protocol/domain communication
send(Conf = #conf{protocol = P}, RoutingId, Message) ->
   P:cast(Conf, RoutingId, Message);
send(Pid, _, Message) ->
   Pid ! Message.

% transform a request into a message and send an asynchronously to the given
% destination returns the request's reference
% Used for INTER-protocol/domain communication
cast(Dst, Request) when is_pid(Dst) ->
   cast(Dst, ?ALL, Request).

cast(Dst, RoutingId, Request) ->
   Ref = make_ref(),
   Msg = {Ref, self(), RoutingId, Request},
   case Dst of
      #conf{protocol = P} -> P:cast(Dst, RoutingId, Msg);
      _ -> Dst ! Msg
   end,
   Ref.


% send an asynchronous request to all the processes of a list
% returns the request's reference
multicast(Pids, Request) ->
   Parent = self(),
   Ref = make_ref(),
   % Each request is tagged with {Ref, Pid} so that when collecting we know
   % exactly which Pids timed out
   [spawn(fun() -> Pid ! {{Ref, Pid}, Parent, ?ALL, Request} end) || Pid <- Pids],
   {Ref, Pids}.


% wait for a response for a previously cast request until timeout occurs
collect(Ref, Timeout) ->
   receive
      {Ref, Result} -> {ok, Result}
   after
      Timeout -> {error, timeout}
   end.


% wait for a single response from a multicast request
collectany({Ref, Pids}, Timeout) ->
   collectMany(Ref, Pids, [], 1, Timeout).


% wait for some responses from a multicast request
collectmany({Ref, Pids}, NumResponses, Timeout) ->
   collectMany(Ref, Pids, [], NumResponses, Timeout).


% wait for all responses from a multicast request
collectall({Ref, Pids}, Timeout) ->
   collectMany(Ref, Pids, [], length(Pids), Timeout).



%%%%%%%%%%%%%%%%%%%%%%
% Sync Communication %
%%%%%%%%%%%%%%%%%%%%%%


% send synchronous request to a process
call(Pid, Request, Retry) when is_pid(Pid) ->
   call(Pid, ?ALL, Request, Retry).

call(Conf = #conf{protocol = P}, RId, Request, Retry) ->
   P:call(Conf, RId, Request, Retry);
call(Pid, RId, Request, Retry) ->
   call(Pid, make_ref(), RId, Request, Retry).


% send parallel requests to all processes in a list and wait for one response
anycall(Pids, Request, Retry) ->
   multicall(Pids, Request, 1, Retry).


% send parallel requests to all processes in a list and wait for all responses
multicall(Pids, Request, Retry) ->
   multicall(Pids, Request, length(Pids), Retry).



% send parallel requests to all processes and wait to get NumResponses responses
multicall(Pids, Request, NumResponses, Retry) ->
   Parent = self(),
   Ref = make_ref(),
   % spawn a collector process so that parent's inbox isn't jammed with unwanted
   % messages beyond the required NumResponses
   % FIXME: this is less efficient than just using the existing process
   spawn(fun() ->
            Collector = self(),
            % create a sub-process for each Pid to make a call
            [spawn(fun() ->
                     Collector ! {{Ref, Pid}, call(Pid, Ref, ?ALL, Request, Retry)}
                  end) || Pid <- Pids],
            Parent ! {Ref, collectMany(Ref, Pids, [], NumResponses, infinity)}
      end),
   % wait for a response from the collector
   receive
      {Ref, Results} -> Results
   end.


%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%


% send synchronous request to a given Pid
call(Pid, Ref, RoutingId, Request, RetryAfter) ->
   Pid ! {Ref, self(), RoutingId, Request},

   receive
      {Ref, Result} -> Result
   after
      RetryAfter -> call(Pid, Ref, RoutingId, Request, RetryAfter)
   end.


% Collect a required number of responses and return them
collectMany(_, _, Responses, 0, _) ->
   {ok, Responses};
collectMany(_, [], Responses, _, _) ->
   {ok, Responses};
collectMany(Ref, RemPids, Responses, Required, Timeout) ->
   receive
      {{Ref, Pid}, Result} ->
         case lists:member(Pid, RemPids) of
            true ->
               collectMany(Ref, lists:delete(Pid, RemPids), [{Pid, Result} |
                     Responses], Required - 1, Timeout);

            false ->    % could be a retransmission
               collectMany(Ref, RemPids, Responses, Required, Timeout)
         end
   after
      Timeout ->
         {timeout, Responses}    % Return responses so far
   end.
