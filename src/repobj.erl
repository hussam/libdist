-module(repobj).
-export([
      new/3,
      do/3,
      fork/4,
      reconfigure/3,
      stop/4
   ]).

-include("repobj.hrl").

% Create a new replicated object
new(CoreSettings = {Module, _Args}, {RepProtocol, RepArgs, Nodes}, Retry) ->
   % spawn new replicas
   Replicas = [
      spawn(N, RepProtocol, new_replica, [CoreSettings, RepArgs]) ||
      N <- Nodes
   ],
   % create a configuration and inform all the replicas of it
   Conf0 = #conf{protocol = RepProtocol, core_module = Module, version = 0},
   RepProtocol:reconfigure(Conf0, Replicas, Retry).     % returns the new config


% Execute a command synchronously on a replicated object
do(Obj = #conf{protocol = Module}, Command, Retry) ->
   Module:do(Obj, Command, Retry).

reconfigure(Obj = #conf{protocol = Module}, NewReplicas, Retry) ->
   Module:reconfigure(Obj, NewReplicas, Retry).

fork(Obj = #conf{protocol = Module}, N, Node, Args) ->
   Module:fork(Obj, N, Node, Args).

stop(Obj = #conf{protocol = Module}, N, Reason, Retry) ->
   Module:stop(Obj, N, Reason, Retry).


