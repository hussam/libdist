-module(repobj).

% Interface for manipulating a replicated object
-export([
      new/3,
      cast/2,
      call/3,
      reconfigure/4,
      stop/4
   ]).

-include("libdist.hrl").

% Create a new replicated object
new(CoreSettings, {RepProtocol, RepArgs, Nodes}, Timeout) ->
   RepProtocol:new(CoreSettings, RepArgs, Nodes, Timeout).

% Send an asynchronous command to a replicated object
cast(Obj = #rconf{protocol = Module}, Command) ->
   Module:cast(Obj, Command).

% Send a synchronous command to a replicated object
call(Obj = #rconf{protocol = Module}, Command, Timeout) ->
   Module:call(Obj, Command, Timeout).

reconfigure(Obj = #rconf{protocol = Module}, NewReplicas, NewArgs, Timeout) ->
   Module:reconfigure(Obj, NewReplicas, NewArgs, Timeout).

stop(Obj = #rconf{protocol = Module}, N, Reason, Timeout) ->
   Module:stop(Obj, N, Reason, Timeout).

