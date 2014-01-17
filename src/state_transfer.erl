% Background and on-demand state transfer
-module(state_transfer).

-export([
      request_blocking/2,
      serve_blocking/3,
      serve_nonblocking/4
   ]).

-include("helper_macros.hrl").


% Request initiating a state transfer from the specified destination server.
% 'AppRequestMsg' is an application-specific message sent to the server to
% initiate the transfer.
request_blocking(Server, AppRequestMsg) ->
   Parent = self(),
   {_Pid, MRef} = spawn_monitor(fun() ->
            ?SEND(Server, {AppRequestMsg, self()}, true),
            State = client_loop(<<>>),
            Parent ! {ok, State}
      end),
   receive
      {ok, State} -> {ok, State};
      {'DOWN', MRef, _, _, Info} when Info /= normal -> {error, Info}
   end.


% Handle a client request and serve the specified state to the Destination.
% Assumes that a client request was already sent, and this function was called
% in response.
serve_blocking(Destination, State, MTU) ->
   {_Pid, MRef} = spawn_monitor(fun() ->
            BinState = term_to_binary(State),
            Size = byte_size(BinState),
            MsgPosLens = case Size rem MTU of
               0 ->
                  [{I * MTU, MTU} || I <- lists:seq(0, trunc(Size / MTU) - 1)];
               R ->
                  [{0, R} | [{R + (I * MTU), MTU} ||
                                    I <- lists:seq(0, trunc(Size / MTU) - 1)] ]
            end,
            server_loop(Destination, BinState, MsgPosLens)
      end),
   receive
      {'DOWN', MRef, _, _, normal} -> ok;
      {'DOWN', MRef, _, _, ErrInfo} -> {error, ErrInfo}
   end.


% Serve the specified state to the client in a non-blocking fashion. When the
% transfer is complete, the callback function is invoked with the transfer
% result.
serve_nonblocking(Destination, State, MTU, Callback) ->
   spawn(fun() ->
            Result = serve_blocking(Destination, State, MTU),
            Callback(Result)
      end).


%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%


% Server transmit loop
server_loop(Dst, State, [PosLen = {Pos, _} | Tail]) ->
   ?SEND(Dst, {state_transfer, self(), Pos, binary_part(State, PosLen)}, true),
   receive
      {ack, Pos} -> server_loop(Dst, State, Tail)
      % TODO: FIXME: handle timeouts in state transfer
   end;
server_loop(Dst, _State, []) ->
   ?SEND(Dst, {state_transfer, finished}, true).


% Client receive loop
client_loop(Acc) ->
   receive
      {state_transfer, Server, Pos, Part} ->
         Server ! {ack, Pos},
         client_loop(<<Acc/binary, Part/binary>>);

      {state_transfer, finished} ->
         binary_to_term(Acc)
   end.


