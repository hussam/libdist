-module(repobj).

% Interface for manipulating a replicated object
-export([
      new/3,
      do/3,
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
do(Obj = #rconf{protocol = Module}, Command, Retry) ->
   Module:do(Obj, Command, Retry).

reconfigure(Obj = #rconf{protocol = Module}, NewReplicas, NewArgs, Retry) ->
   Module:reconfigure(Obj, NewReplicas, NewArgs, Retry).

stop(Obj = #rconf{protocol = Module}, N, Reason, Retry) ->
   Module:stop(Obj, N, Reason, Retry).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Interface for examining a repobj configuration %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% XXX: Is it cleaner if these were in a separate module?

version(#rconf{version = Vn}) -> Vn;
version(_) -> not_a_repobj.

pids(#rconf{pids = Pids}) -> Pids;
pids(_) -> not_a_repobj.

protocol(#rconf{protocol = Protocol}) -> Protocol;
protocol(_) -> not_a_repobj.

args(#rconf{args = Args}) -> Args;
args(_) -> not_a_repobj.


