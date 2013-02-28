-module(ldsm).

% This is a behaviour
-export([behaviour_info/1]).

% State Machine interface
-export([
      start/2,
      do/3,
      is_mutating/2,
      stop/2
   ]).

% Helper functions
-export([
      is_rp_protocol/1,
      module/1,
      state/1,
      export/1,
      export/2,
      import/1
   ]).


% Define behaviour callbacks
behaviour_info(callbacks) ->
   [  {init_sm,1},
      {handle_cmd, 3},
      {is_mutating,1},
      {stop, 2},
      {export, 1},
      {import, 1}
   ];
behaviour_info(_) ->
   undefined.

% Start a new state machine server
start(Module, Args) ->
   {
      spawn(fun() -> loop(Module, Module:init_sm(Args)) end),
      Module,
      self()
   }.

is_mutating({_, Module, _}, Command) ->
   Module:is_mutating(Command).

do(SMC, Command, AllowSideEffects) ->
   call(SMC, do, Command, AllowSideEffects).

stop(SMC, Reason) ->
   call(SMC, stop, Reason).

module({_, Module, _}) ->
   Module.

state(SMC) ->
   call(SMC, get_state).

export(SMC = {_, Module, _}) ->
   {Module, call(SMC, export)}.

export(Module, State) ->
   {Module, State}.

import({Module, ExportedState}) ->
   {
      spawn(fun() -> loop(Module, Module:import(ExportedState)) end),
      Module,
      self()
   }.


% is the passed in state machine a libdist replication or partitioning protocol?
is_rp_protocol({_, chain, _}) -> true;
is_rp_protocol({_, primary_backup, _}) -> true;
is_rp_protocol({_, quorum, _}) -> true;
is_rp_protocol(_) -> false.



%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%

loop(Module, State) ->
   receive
      {SMC = {_, _, Client}, do, Command, AllowSideEffects} ->
         case Module:handle_cmd(State, Command, AllowSideEffects) of
            {reply, Reply} ->
               Client ! {SMC, Reply},
               loop(Module, State);

            {reply, Reply, NewState} ->
               Client ! {SMC, Reply},
               loop(Module, NewState);

            {noreply, NewState} ->
               Client ! {SMC, ok},
               loop(Module, NewState);

            noreply ->
               Client ! {SMC, ok},
               loop(Module, State)
         end;

      {SMC = {_, _, Client}, stop, Reason} ->
         Module:stop(State, Reason),
         Client ! {SMC, ok};

      {SMC = {_, _, Client}, export} ->
         Client ! {SMC, Module:export(State)},
         loop(Module, State);

      {SMC = {_, _, Client}, get_state} ->
         Client ! {SMC, State},
         loop(Module, State)
   end.


% make a call to the SM server with 0,1,2 arguments
-compile({inline, [call/2, call/3, call/4]}).

call(SMC = {SM, _, _}, Tag) ->
   SM ! {SMC, Tag},
   receive
      {SMC, Result} -> Result
   end.

call(SMC = {SM, _, _}, Tag, Arg1) ->
   SM ! {SMC, Tag, Arg1},
   receive
      {SMC, Result} -> Result
   end.

call(SMC = {SM, _, _}, Tag, Arg1, Arg2) ->
   SM ! {SMC, Tag, Arg1, Arg2},
   receive
      {SMC, Result} -> Result
   end.


