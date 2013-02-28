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

new(Node, SMModule, SMArgs) ->
   server:start(Node, ?MODULE, {SMModule, SMArgs}).

do(Pid, Command) ->
   libdist_utils:cast(Pid, {command, Command}).

stop(Pid, Reason) ->
   libdist_utils:cast(Pid, {stop, Reason}).


%%%%%%%%%%%%%%%%%%%%%%
% Callback Functions %
%%%%%%%%%%%%%%%%%%%%%%

% Initialize the state of the new replica
init(_Me, {SMModule, SMArgs}) ->
   ldsm:start(SMModule, SMArgs).


% Handle a queued message a standalone replica (i.e. no replication)
handle_msg(Me, Message, SM) ->
   case Message of
      % Handle a command for the core state machine
      {Ref, Client, {command, Command}} ->
         Client ! {Ref, ldsm:do(SM, Command, ?AllowSideEffects)},
         consume;

      % Return the current configuration
      {Ref, Client, get_conf} ->
         Client ! {Ref, #rconf{protocol = ?MODULE, pids = [Me]}},
         consume;

      % Return the state machine module
      {Ref, Client, get_sm_module} ->
         Client ! {Ref, ldsm:module(SM)},
         consume;

      % Stop this replica
      {Ref, Client, {stop, Reason}} ->
         Client ! {Ref, ldsm:stop(SM, Reason)},
         {stop, Reason};

      {Ref, Client, {replace, Me, Conf}} ->
         Client ! {Ref, Conf},
         consume;

      % Return the state machine
      % TODO: change this into some sort of background state transfer
      {Ref, Client, get_sm} ->
         Client ! {Ref, ldsm:export(SM)},
         consume;

      _ ->
         no_match
   end.
