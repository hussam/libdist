-module(core, [Module, Instance]).

% Core interface
-export([
      new/2,
      do/1,
      fork/2,
      is_mutating/1,
      stop/1
   ]).

new(Module, Args) ->
   instance(Module, Module:new(Args)).

do(Command) ->
   Module:do(Instance, Command).

fork(Node, Args) ->
   Module:fork(Instance, Node, Args).

is_mutating(Command) ->
   Module:is_mutating(Command).

stop(Reason) ->
   Module:stop(Instance, Reason).

