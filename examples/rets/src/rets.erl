-module(rets).
-behaviour(ldsm).

% Public API
-export([
      ping/3,
      read/2,
      write/3
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


% Routing functions
-export([
      route/2,
      route_to_first/2
   ]).

% 5 second timeout
-define(TIMEOUT, 5000).

-define(DETS_FILE, "/tmp/rets_dets").


ping(Conf, PingType, Key) ->
   repobj:call(Conf, {PingType, Key}, ?TIMEOUT).

read(Conf, Key) ->
   repobj:call(Conf, {read, Key}, ?TIMEOUT).

write(Conf, Key, Value) ->
   repobj:call(Conf, {write, Key, Value}, ?TIMEOUT).


% Select the partition that can handle the given command
route({_OP, Key}, Partitions) -> do_route(Key, Partitions);
route({write, Key, _Value}, Partitions) -> do_route(Key, Partitions).

do_route(Key, []) ->
   error("could not route key to proper partition", Key);
do_route(Key, [P = {{Begin, End}, _} | _]) when Key >= Begin, Key < End ->
   P;
do_route(Key, [_ | Tail]) ->
   do_route(Key, Tail).


route_to_first(_Key, [Hd | _]) -> Hd.




%%%%%%%%%%%%%%%%%%
% ldsm Callbacks %
%%%%%%%%%%%%%%%%%%


% the functions below are just implementing the ldsm callback interface
init_sm(Options) ->
   file:delete(?DETS_FILE),
   {ok, Tab} = dets:open_file(bench_dets, [{file, ?DETS_FILE} | Options]),
   Tab.

handle_cmd(_, {rping, _}, _) -> {reply, ok};
handle_cmd(_, {wping, _}, _) -> {reply, ok};
handle_cmd(Tab, {read, Key}, _) -> {reply, dets:lookup(Tab, Key)};
handle_cmd(Tab, {write, Key, Value}, _) -> {reply, dets:insert(Tab, {Key, Value})};
handle_cmd(_, _, _) -> {reply, undefined_op}.

is_mutating({rping, _}) -> false;
is_mutating({wping, _}) -> true;
is_mutating({read, _}) -> false;
is_mutating({write, _, _}) -> true.

stop(Tab, _Reason) -> dets:close(Tab).

% Export the state of the state machine
export(Tab, _Tag) -> export(Tab).
export(Tab) ->
   Tuples = lists:flatten(dets:match(Tab, '$1')),
   MinNoSlots = {min_no_slots, element(3, dets:info(Tab, no_slots))},
   RamFile    = {ram_file, dets:info(Tab, ram_file)},
   AutoSave   = {auto_save, dets:info(Tab, auto_save)},
   Type       = {type, dets:info(Tab, type)},
   {Tuples, [MinNoSlots, RamFile, AutoSave, Type]}.


import({Tuples, DetsOptions}) ->
   file:delete(?DETS_FILE),
   {ok, Tab} = dets:open_file(bench_dets, [{file, ?DETS_FILE} | DetsOptions]),
   dets:insert(Tab, Tuples),
   Tab.
