-module(kvs).
-behaviour(ldsm).

% Echo Public API
-export([
      put/3,
      get/2
   ]).

% libdist state machine behaviour interface
-export([
      init_sm/1,
      handle_cmd/3,
      is_mutating/1,
      stop/2,
      export/1,
      import/1
   ]).

% 5 second timeout
-define(TIMEOUT, 5000).


% Bind a value to a key
put(Conf, Key, Value) ->
   libdist_utils:call(Conf, {put, Key, Value}, ?TIMEOUT).

% Get the value bound to a key
get(Conf, Key) ->
   libdist_utils:call(Conf, {get, Key}, ?TIMEOUT).




% All the functions below are just implementing the state machine interface


% Initialize the local state machine
init_sm(_Args) ->
   dict:new().

% Handle put/get commands
handle_cmd(Dict, {get, Key}, _) ->
   {reply, dict:find(Key, Dict)};
handle_cmd(Dict, {put, Key, Value}, _) ->
   {reply, ok, dict:store(Key, Value, Dict)};
handle_cmd(_, _, _) ->
   {reply, undefined_op}.

% Does the command change the local state or not?
is_mutating({put, _}) ->
   true;
is_mutating(_) ->
   false.


% Export the state of the state machine
export(Dict) ->
   dict:to_list(Dict).


% Import a previously exported state
import(KVList) ->
   dict:from_list(KVList).


% Stop the state machine
stop(_, Reason) ->
   io:format("Stopping because of: ~p\n", [Reason]).  % no actual cleanup needed here


