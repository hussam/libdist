-module(repobj).

% Interface for manipulating a replicated object
-export([
      new/3,
      do/3,
      fork/4,
      reconfigure/4,
      stop/4
   ]).

% Interface for examining a configuration
-export([
      version/1,
      pids/1,
      protocol/1,
      args/1
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



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Interface for examining a repobj configuration %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% XXX: Is it cleaner if these were in a separate module?

version(#conf{version = Vn}) -> Vn.

pids(#conf{pids = Pids}) -> Pids.

protocol(#conf{protocol = Protocol}) -> Protocol.

args(#conf{args = Args}) -> Args.


