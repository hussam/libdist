-module(singleton).
-export([
      new/3,
      do/2,
      stop/2
   ]).

% Server callbacks
-export([
      init/2,
      handle_msg/3
   ]).

-include("libdist.hrl").

-define(AllowSideEffects, true).

new(Node, CoreModule, CoreArgs) ->
   server:start(Node, ?MODULE, {CoreModule, CoreArgs}).

do(Pid, Command) ->
   libdist_utils:cast(Pid, {command, Command}).

stop(Pid, Reason) ->
   libdist_utils:cast(Pid, {stop, Reason}).


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
      {Ref, Client, {command, Command}} ->
         Client ! {Ref, Core:do(?AllowSideEffects, Command)},
         consume;

      % Return the current configuration
      {Ref, Client, get_conf} ->
         Client ! {Ref, #rconf{protocol = ?MODULE, pids = [Me]}},
         consume;

      % Stop this replica
      {Ref, Client, {stop, Reason}} ->
         Client ! {Ref, Core:stop(Reason)},
         {stop, Reason};

      _ ->
         no_match
   end.
