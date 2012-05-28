-module(repobj).
-export([
      new/3,
      do/3,
      fork/4,
      reconfigure/4,
      stop/4
   ]).

-include("repobj.hrl").

% Create a new replicated object
new(CoreSettings, {RepProtocol, RepArgs, Nodes}, Retry) ->
   RepProtocol:new(CoreSettings, RepArgs, Nodes, Retry).

% Execute a command synchronously on a replicated object
do(Obj = #conf{protocol = Module}, Command, Retry) ->
   Module:do(Obj, Command, Retry).

reconfigure(Obj = #conf{protocol = Module}, NewReplicas, NewArgs, Retry) ->
   Module:reconfigure(Obj, NewReplicas, NewArgs, Retry).

fork(Obj = #conf{protocol = Module}, N, Node, Args) ->
   Module:fork(Obj, N, Node, Args).

stop(Obj = #conf{protocol = Module}, N, Reason, Retry) ->
   Module:stop(Obj, N, Reason, Retry).


