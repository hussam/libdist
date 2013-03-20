-module(repobj).

% Interface for manipulating a replicated object
-export([
      new/4,
      inherit/4,
      cast/2,
      call/3,
      reconfigure/4,
      stop/4
   ]).

-include("libdist.hrl").


% Create a new replicated state machine
new(PSettings={PModule, PArgs}, SMSettings={SMModule, _} , Nodes, Timeout) ->
   % spawn new replicas
   Replicas = [ replica:new(PSettings, SMSettings, N) || N <- Nodes ],
   % configure replicas and return configuration
   reconfigure(#rconf{protocol=PModule, args=SMModule}, Replicas, PArgs, Timeout).


% Create a new replicated state machine and prepare it to later inherit the
% state of an existing process
inherit(Pid, PSettings={PModule, PArgs}, Nodes, Timeout) ->
   % spawn new replicas
   Replicas = [ replica:new(no_sm, PSettings, N) || N <- Nodes ],
   % get the process's state machine module
   SMModule = libdist_utils:call(Pid, get_sm_module, Timeout),
   % create a configuration and inform all the replicas of it and return it
   reconfigure(#rconf{protocol=PModule, args=SMModule}, Replicas, PArgs, Timeout).


% Send an asynchronous command to a replicated object
cast(Conf=#rconf{protocol = P}, Command) ->
   P:cast(Conf, Command).


% Send a synchronous command to a replicated object
call(Conf = #rconf{protocol = P}, Command, Timeout) ->
   libdist_utils:collect(P:cast(Conf, Command), Timeout).


% Reconfigure the replicated object with a new set of replicas
reconfigure(OldConf, NewReplicas, NewArgs, Timeout) ->
   #rconf{version=Vn, protocol=P, pids=OldReplicas, args=OldArgs} = OldConf,
   NewConf = OldConf#rconf{
      version = Vn + 1,
      pids = NewReplicas,
      args = P:make_conf_args(OldArgs, NewArgs)
   },
   % This takes out the replicas in the old configuration but not in the new one
   Refs = libdist_utils:multicast(OldReplicas, {reconfigure, NewConf}),
   case libdist_utils:collectall(Refs, Timeout) of
      {ok, _} ->
         % This integrates replicas in the new configuration that are not old
         Refs2 = libdist_utils:multicast(NewReplicas, {reconfigure, NewConf}),
         case libdist_utils:collectall(Refs2, Timeout) of
            {ok, _} ->
               {ok, NewConf};
            Error ->
               Error
         end;
      % TODO: distinguish which case of timeout occurred
      Error ->
         Error
   end.


% Stop one of the replicas of the replicated object.
stop(Obj=#rconf{version = Vn, pids = OldReplicas}, N, Reason, Timeout) ->
   Pid = lists:nth(N, OldReplicas),
   case libdist_utils:collect(libdist_utils:cast(Pid, {stop, Reason}), Timeout) of
      {ok, _} ->
         NewReplicas = lists:delete(Pid, OldReplicas),
         NewConf = Obj#rconf{version = Vn + 1, pids = NewReplicas},
         Refs = libdist_utils:multicast(NewReplicas, {reconfigure, NewConf}),
         case libdist_utils:collectall(Refs, Timeout) of
            {ok, _} ->
               {ok, NewConf};
            Error ->
               Error
         end;
      % TODO: distinguish which case of timeout occurred
      Error ->
         Error
   end.
