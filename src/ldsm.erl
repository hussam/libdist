-module(ldsm).

% This is a behaviour
-export([behaviour_info/1]).

% State Machine interface
-export([
      start/2,
      do/3,
      do/5,
      is_mutating/2,
      stop/2
   ]).

% Helper functions
-export([
      is_rp_protocol/1,
      module/1,
      export/1,
      export/2,
      import/1,
      wrap/2,
      get_state/1,
      set_state/2
   ]).

-include("helper_macros.hrl").


% Define behaviour callbacks
behaviour_info(callbacks) ->
   [  {init_sm,1},
      {handle_cmd, 3},
      {is_mutating,1},
      {stop, 2},
      {export, 1},
      {export, 2},
      {import, 1}
   ];
behaviour_info(_) ->
   undefined.

% Start a new state machine server
start(Module, Args) ->
   {
      spawn(fun() -> loop(Module, Module:init_sm(Args)) end),
      Module
   }.

is_mutating({_, Module}, Command) ->
   Module:is_mutating(Command).

do(SM, Command, AllowSideEffects) ->
   call(SM, do, Command, AllowSideEffects).

do(SM, Ref, Client, Command, AllowSideEffects) ->
   call(SM, do, Ref, Client, Command, AllowSideEffects).

stop(SM, Reason) ->
   call(SM, stop, Reason).

module({_, Module}) ->
   Module.

export(SM = {_, Module}) ->
   {exported_sm, Module, call(SM, export)}.

export(SM = {_, Module}, Tag) ->
   {exported_sm, Module, call(SM, export, Tag)}.

wrap(Module, State) ->
   {exported_sm, Module, State}.

import({exported_sm, Module, ExportedState}) ->
   {
      spawn(fun() -> loop(Module, Module:import(ExportedState)) end),
      Module
   }.

get_state({exported_sm, _, State}) -> State;    % for already exported state
get_state(SM) -> call(SM, get_state).           % for a constructed/existing SM


set_state({exported_sm, Module, _}, NewState) -> {exported_sm, Module, NewState};
set_state(SM, NewState) -> call(SM, set_state, NewState), SM.


% is the passed in state machine a libdist replication or partitioning protocol?
is_rp_protocol({_, replica}) -> true;
is_rp_protocol({exported_sm, replica, _}) -> true;   % for exported state
is_rp_protocol(_) -> false.



%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%

loop(Module, State) ->
   receive
      {Caller, do, Ref, Client, Command, AllowSideEffects} ->
         case Module:handle_cmd(State, Command, AllowSideEffects) of
            {reply, Reply} ->
               case AllowSideEffects of
                  true -> Client ! {Ref, Reply};
                  false -> do_nothing
               end,
               Caller ! done,
               loop(Module, State);

            {reply, Reply, NewState} ->
               case AllowSideEffects of
                  true -> Client ! {Ref, Reply};
                  false -> do_nothing
               end,
               Caller ! done,
               loop(Module, NewState);

            {noreply, NewState} ->
               Caller ! done,
               loop(Module, NewState);

            noreply ->
               Caller ! done,
               loop(Module, State)
         end;

      {Caller, do, Command, AllowSideEffects} ->
         case Module:handle_cmd(State, Command, AllowSideEffects) of
            {reply, Reply} ->
               Caller ! Reply,
               loop(Module, State);

            {reply, Reply, NewState} ->
               Caller ! Reply,
               loop(Module, NewState);

            {noreply, NewState} ->
               Caller ! done,
               loop(Module, NewState);

            noreply ->
               Caller ! done,
               loop(Module, State)
         end;

      {Caller, stop, Reason} ->
         Module:stop(State, Reason),
         Caller ! ok;

      {Caller, export} ->
         Caller ! Module:export(State),
         loop(Module, State);

      {Caller, export, Tag} ->
         Caller ! Module:export(State, Tag),
         loop(Module, State);

      {Caller, get_state} ->
         Caller ! State,
         loop(Module, State);

      {Caller, set_state, NewState} ->
         Caller ! ok,
         loop(Module, NewState)
   end.


% make a call to the SM server with 0,1,2,4 arguments
-compile({inline, [call/2, call/3, call/4, call/6]}).

call({Server, _}, Tag) ->
   Server ! {self(), Tag},
   receive
      Result -> Result
   end.

call({Server, _}, Tag, Arg1) ->
   Server ! {self(), Tag, Arg1},
   receive
      Result -> Result
   end.

call({Server, _}, Tag, Arg1, Arg2) ->
   Server ! {self(), Tag, Arg1, Arg2},
   receive
      Result -> Result
   end.

call({Server, _}, Tag, Arg1, Arg2, Arg3, Arg4) ->
   Server ! {self(), Tag, Arg1, Arg2, Arg3, Arg4},
   receive
      Result -> Result
   end.
