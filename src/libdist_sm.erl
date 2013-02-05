-module(libdist_sm, [Module, Instance]).

% State Machine interface
-export([
      new/2,
      do/1,
      is_mutating/1,
      stop/1
   ]).

new(Module, Args) ->
   instance(Module, Module:new(Args)).

do(Command) ->
   Module:do(Instance, Command).

is_mutating(Command) ->
   Module:is_mutating(Command).

stop(Reason) ->
   Module:stop(Instance, Reason).
