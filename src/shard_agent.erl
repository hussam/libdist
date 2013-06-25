-module(shard_agent).
-behaviour(ldsm).


% State machine interface
-export([
      init_sm/1,
      handle_cmd/3,
      is_mutating/1,
      stop/2,
      export/1,
      export/2,
      import/1
   ]).

-include("libdist.hrl").


%%%%%%%%%%%%%%%%%%%%%%%%%%%
% State Machine Callbacks %
%%%%%%%%%%%%%%%%%%%%%%%%%%%


init_sm(Conf) ->
   Conf.

handle_cmd(State = #conf{protocol = P}, Command, AllowSideEffects) ->
   Ref = P:cast(State, Command),
   % TODO: handle timeouts
   receive
      {Ref, Result} when AllowSideEffects -> {reply, Result};
      {Ref, _Result} -> noreply
   end.

% XXX: This is potentially not a problem because clients check read/write cmds
% using the module in their #conf{}, and this module is not likely to end up in
% a configuration. TODO: Check that this is actually true!
is_mutating(_Command) ->
   true.

stop(_State = #conf{partitions = Ps}, Reason) ->
   % TODO: handle timeouts
   libdist_utils:collectall(
      libdist_utils:multicast([Pid || {_,Pid} <- Ps], {stop, Reason}),
      infinity
   ).


% Nothing much to import/export
export(State) -> State.
export(State, _Tag) -> export(State).
import(State) -> State.

