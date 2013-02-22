-module(singleton).
-export([
      new/4,
      new_replica/3,
      do/3,
      reconfigure/4,
      stop/4
   ]).

% Server callbacks
-export([
      init/2,
      handle_msg/3
   ]).

-include("repobj.hrl").

-define(AllowSideEffects, true).

new(CoreSettings, _Args, [Node], _Retry) ->
   Replica = [ new_replica(Node, CoreSettings, _Args) ],
   #rconf{protocol = ?MODULE, version = 1, pids = Replica}.  % return config

new_replica(Node, CoreSettings, _RepArgs) ->
   server:start(Node, ?MODULE, CoreSettings).

do(_Obj=#rconf{pids=[Pid]}, Command, Retry) ->
   libdist_utils:call(Pid, command, Command, Retry).

reconfigure(OldConf, NewReplica, _NewArgs, Retry) ->
   #rconf{version = Vn, pids = [OldReplica]} = OldConf,
   NewConf = OldConf#rconf{version = Vn + 1, pids = [NewReplica]},
   libdist_utils:call(OldReplica, reconfigure, NewConf, Retry),
   if
      NewReplica == OldReplica -> do_nothing;
      true -> libdist_utils:call(NewReplica, reconfigure, NewConf, Retry)
   end,
   NewConf.

stop(Obj, N, Reason, Retry) ->
   Pid = lists:nth(N, Obj#rconf.pids),
   libdist_utils:call(Pid, stop, Reason, Retry).


%%%%%%%%%%%%%%%%%%%%%%
% Callback Functions %
%%%%%%%%%%%%%%%%%%%%%%

% Initialize the state of the new replica
init(_Me, {CoreModule, CoreArgs}) ->
   Core = libdist_sm:new(CoreModule, CoreArgs),
   Conf = #rconf{protocol = ?MODULE, version = 1, pids = [self()]},
   {Core, Conf}.


% Handle a queued message a standalone replica (i.e. no replication)
handle_msg(_Me, Message, {Core, Conf}) ->
   case Message of
      % Handle a command for the core
      {Ref, Client, command, Command} ->
         Client ! {Ref, Core:do(?AllowSideEffects, Command)},
         consume;

      % Reconfigure this replica. This is meaningless here.
      {Ref, Client, reconfigure, _} ->
         Client ! {Ref, ok},
         {consume, {Core, Conf#rconf{version = Conf#rconf.version + 1}}};

      % Return the current configuration
      {Ref, Client, get_conf} ->
         Client ! {Ref, Conf},
         consume;

      % Stop this replica
      {Ref, Client, stop, Reason} ->
         Client ! {Ref, Core:stop(Reason)},
         {stop, Reason};

      _ ->
         no_match
   end.
