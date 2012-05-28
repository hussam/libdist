-module(singleton).
-export([
      new/4,
      new_replica/2,
      do/3,
      fork/4,
      reconfigure/4,
      stop/4
   ]).

-include("repobj.hrl").

new(CoreSettings, _Args, [Node], _Retry) ->
   Replica = [ spawn(Node, ?MODULE, new_replica, [CoreSettings, _Args]) ],
   #conf{protocol = ?MODULE, version = 1, pids = Replica}.  % return config

new_replica({CoreModule, CoreArgs}, _RepArgs) ->
   Core = core:new(CoreModule, CoreArgs),
   Conf = #conf{protocol = ?MODULE, version = 1, pids = [self()]},
   loop(Core, Conf).

do(_Obj=#conf{pids=[Pid]}, Command, Retry) ->
   repobj_utils:call(Pid, command, Command, Retry).

fork(Obj, N, Node, Args) ->
   Pid = lists:nth(N, Obj#conf.pids),
   repobj_utils:cast(Pid, fork, {Node, Args}).

reconfigure(OldConf, NewReplica, _NewArgs, Retry) ->
   #conf{version = Vn, pids = [OldReplica]} = OldConf,
   NewConf = OldConf#conf{version = Vn + 1, pids = [NewReplica]},
   repobj_utils:call(OldReplica, reconfigure, NewConf, Retry),
   if
      NewReplica == OldReplica -> do_nothing;
      true -> repobj_utils:call(NewReplica, reconfigure, NewConf, Retry)
   end,
   NewConf.

stop(Obj, N, Reason, Retry) ->
   Pid = lists:nth(N, Obj#conf.pids),
   repobj_utils:call(Pid, stop, Reason, Retry).


%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%


% Main loop for a standalone replica (i.e. no replication)
loop(Core, Conf) ->
   receive
      % Handle a command for the core
      {Ref, Client, command, Command} ->
         Client ! {Ref, Core:do(Command)},
         loop(Core, Conf);

      % Fork this replica
      {Ref, Client, fork, {ForkNode, ForkArgs}} ->
         ForkedPid = spawn(ForkNode, fun() ->
                  loop(Core:fork(ForkNode, ForkArgs), Conf)
            end),
         Client ! {Ref, ForkedPid},
         loop(Core, Conf);

      % Reconfigure this replica. This is meaningless here.
      {Ref, Client, reconfigure, _} ->
         Client ! {Ref, ok},
         loop(Core, Conf#conf{version = Conf#conf.version + 1});

      % Return the current configuration
      {Ref, Client, get_conf} ->
         Client ! {Ref, Conf},
         loop(Core, Conf);

      % Stop this replica
      {Ref, Client, stop, Reason} ->
         Client ! {Ref, Core:stop(Reason)};

      % Unexpected message
      UnexpectedMessage ->
         io:format("Received unexpected message ~p at ~p (~p)\n",
            [UnexpectedMessage, self(), ?MODULE])
   end.
