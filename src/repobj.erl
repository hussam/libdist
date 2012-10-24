-module(repobj).

% Interface for manipulating a replicated object
-export([
      new/3,
      do/3,
      reconfigure/4,
      stop/4
   ]).

-include("repobj.hrl").

% Create a new replicated object
new(CoreSettings, {RepProtocol, RepArgs, Nodes}, Retry) ->
   RepProtocol:new(CoreSettings, RepArgs, Nodes, Retry).

% Execute a command synchronously on a replicated object
do(Obj = #rconf{protocol = Module}, Command, Retry) ->
   Module:do(Obj, Command, Retry).

reconfigure(Obj = #rconf{protocol = Module}, NewReplicas, NewArgs, Retry) ->
   Module:reconfigure(Obj, NewReplicas, NewArgs, Retry).

stop(Obj = #rconf{protocol = Module}, N, Reason, Retry) ->
   Module:stop(Obj, N, Reason, Retry).

