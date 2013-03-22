-module(kvs).
-behaviour(ldsm).

% Echo Public API
-export([
      put/3,
      get/2,
      split/1,
      route/2
   ]).

% libdist state machine behaviour interface
-export([
      init_sm/1,
      handle_cmd/3,
      is_mutating/1,
      stop/2,
      export/1,
      export/2,
      import/1
   ]).

% 5 second timeout
-define(TIMEOUT, 5000).


% Bind a value to a key
put(Conf, Key, Value) ->
   repobj:call(Conf, Key, {put, Key, Value}, ?TIMEOUT).

% Get the value bound to a key
get(Conf, Key) ->
   repobj:call(Conf, Key, {get, Key}, ?TIMEOUT).


% Returns a function that given a tag, will split it into NumPartitions partitions
split(NumPartitions) ->
   fun
      ([]) -> split({0, 1}, NumPartitions);
      (Tags) -> split(lists:last(Tags), NumPartitions)
   end.
split({Begin, End}, NumPartitions) when Begin >= 0, End =< 1 ->
   Step = (End - Begin) / NumPartitions,
   [ {{Begin + (I * Step), Begin + ((I+1) * Step)}, node()} ||
      I <- lists:seq(0,NumPartitions - 1) ];
split(_, _) ->
   error("Range must be between 0 and 1").


% Select the partition that can handle the given command
route(Key, Partitions) ->
   do_route(Key, Partitions).

do_route(Key, []) ->
   error("could not route key to proper partition", Key);
do_route(Key, [P = {{Begin, End}, _} | _]) when Key >= Begin, Key < End ->
   P;
do_route(Key, [_ | Tail]) ->
   do_route(Key, Tail).



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
is_mutating({put, _, _}) ->
   true;
is_mutating({get, _}) ->
   false.


% Export the state of the state machine
export(Dict) ->
   dict:to_list(Dict).

export(Dict, {Begin, End}) ->
   dict:to_list(dict:filter(fun(K,_) -> (K >= Begin) and (K < End) end, Dict)).



% Import a previously exported state
import(KVList) ->
   dict:from_list(KVList).


% Stop the state machine
stop(_, _Reason) ->
   ok.      % no actual cleanup needed here


