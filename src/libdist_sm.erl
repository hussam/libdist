-module(libdist_sm, [Module, Instance]).

% Expects the SM to define the following interface
% -export([
%       new/1,
%       handle_cmd/3,
%       is_mutating/1,
%       stop/2
%    ]).


% State Machine interface
-export([
      new/2,
      do/2,
      is_mutating/1,
      stop/1
   ]).

new(Module, Args) ->
   instance(Module, Module:new(Args)).

do(AllowSideEffects, Command) ->
   Module:handle_cmd(Instance, AllowSideEffects, Command).

is_mutating(Command) ->
   Module:is_mutating(Command).

stop(Reason) ->
   Module:stop(Instance, Reason).
