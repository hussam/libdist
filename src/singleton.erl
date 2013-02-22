-module(singleton).
-export([
      new/3,
      do/3,
      stop/3
   ]).

% Server callbacks
-export([
      init/2,
      handle_msg/3
   ]).

-include("repobj.hrl").

-define(AllowSideEffects, true).

new(Node, CoreModule, CoreArgs) ->
   server:start(Node, ?MODULE, {CoreModule, CoreArgs}).

do(Pid, Command, Retry) ->
   libdist_utils:call(Pid, command, Command, Retry).

stop(Pid, Reason, Retry) ->
   libdist_utils:call(Pid, stop, Reason, Retry).


%%%%%%%%%%%%%%%%%%%%%%
% Callback Functions %
%%%%%%%%%%%%%%%%%%%%%%

% Initialize the state of the new replica
init(_Me, {CoreModule, CoreArgs}) ->
   libdist_sm:new(CoreModule, CoreArgs).


% Handle a queued message a standalone replica (i.e. no replication)
handle_msg(Me, Message, Core) ->
   case Message of
      % Handle a command for the core
      {Ref, Client, command, Command} ->
         Client ! {Ref, Core:do(?AllowSideEffects, Command)},
         consume;

      % Return the current configuration
      {Ref, Client, get_conf} ->
         Client ! {Ref, #rconf{protocol = ?MODULE, pids = [Me]}},
         consume;

      % Stop this replica
      {Ref, Client, stop, Reason} ->
         Client ! {Ref, Core:stop(Reason)},
         {stop, Reason};

      _ ->
         no_match
   end.
