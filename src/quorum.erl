-module(quorum).
-export([
      new/4,
      new_replica/2,
      do/3,
      fork/4,
      reconfigure/4,
      stop/4
   ]).

-include("repobj.hrl").

-record(state, {
      core,
      conf,
      others = [],
      updates_count = 0
   }).

% Create a new chain replicated state machine
new(CoreSettings = {Module, _}, QArgs, Nodes, Retry) ->
   % spawn new replicas
   Replicas = [
      spawn(N, ?MODULE, new_replica, [CoreSettings, QArgs]) || N <- Nodes ],
   % compute the read/write quorum sizes
   N = length(Replicas),
   R = case proplists:lookup(r, QArgs) of
      {r, ReadQuorumSize} -> ReadQuorumSize;
      none -> trunc(N/2) + 1
   end,
   W = case proplists:lookup(w, QArgs) of
      {w, WriteQuorumSize} -> WriteQuorumSize;
      none -> trunc(N/2) + 1
   end,
   % create a configuration and inform all the replicas of it
   Conf0 = #conf{protocol = ?MODULE, args = {Module, R, W}, version = 0},
   reconfigure(Conf0, Replicas, [], Retry).   % returns the new configuration


% Start a new replica
new_replica({CoreModule, CoreArgs}, _RepArgs) ->
   State = #state{
      core = sm:new(CoreModule, CoreArgs),
      conf = #conf{protocol = ?MODULE}
   },
   loop(State).

% Send a command to a replicated object
do(#conf{pids = Replicas, args = {CoreModule, R, W}}, Command, Retry) ->
   {QName, QSize} = case CoreModule:is_mutating(Command) of
      true -> {w, W};
      false -> {r, R}
   end,
   maxResponse([ Response || {_Pid, Response} <-
         repobj_utils:multicall(Replicas, QName, Command, QSize, Retry) ]).

% Fork one of the replicas in this replicated object
fork(Obj, N, Node, Args) ->
   Pid = lists:nth(N, Obj#conf.pids),
   repobj_utils:cast(Pid, fork, {Node, Args}).

% Reconfigure the replicated object with a new set of replicas
reconfigure(OldConf, NewPids, NewArgs, Retry) ->
   #conf{version = OldVn, pids = OldPids, args = {CMod, OldR, OldW}} = OldConf,
   % recompute the read/write quorum sizes
   NewN = length(NewPids),
   NewR = case proplists:lookup(r, NewArgs) of
      {r, ReadQuorumSize} -> ReadQuorumSize;
      none when OldR > NewN -> trunc(NewN/2) + 1;
      _ -> OldR
   end,
   NewW = case proplists:lookup(w, NewArgs) of
      {w, WriteQuorumSize} -> WriteQuorumSize;
      none when OldW > NewN -> trunc(NewN/2) + 1;
      _ -> OldW
   end,
   % the new configuration
   NewConf = OldConf#conf{
      version = OldVn + 1,
      pids = NewPids,
      args = {CMod, NewR, NewW}
   },
   % This takes out the replicas in the old configuration but not in the new one
   repobj_utils:multicall(OldPids, reconfigure, NewConf, Retry),
   % This integrates the replicas in the new configuration that are not old
   repobj_utils:multicall(NewPids, reconfigure, NewConf, Retry),
   NewConf.    % return the new configuration

% Stop one of the replicas of the replicated object.
stop(Obj=#conf{version = Vn, pids = OldReplicas}, N, Reason, Retry) ->
   Pid = lists:nth(N, OldReplicas),
   repobj_utils:call(Pid, stop, Reason, Retry),
   NewReplicas = lists:delete(Pid, OldReplicas),
   NewConf = Obj#conf{version = Vn + 1, pids = NewReplicas},
   repobj_utils:multicall(NewReplicas, reconfigure, NewConf, Retry),
   NewConf.



%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%

loop(State = #state{
      core = Core,
      conf = Conf,
      updates_count = UpdatesCount
   }) ->
   receive
      % Respond to a command as a member of a read quorum
      {Ref, Coordinator, r, Command} ->
         Coordinator ! {Ref, {UpdatesCount, Core:do(Command)}},
         loop(State);

      % Respond to a command as a member of a write quorum
      {Ref, Coordinator, w, Command} ->
         NewCount = UpdatesCount + 1,
         Coordinator ! {Ref, {NewCount, Core:do(Command)}},
         loop(State#state{updates_count = NewCount});

      % Fork this replica
      {Ref, Client, fork, {ForkNode, ForkArgs}} ->
         % fork the local core
         ForkedState = State#state{
            core = Core:fork(ForkNode, ForkArgs),
            conf = undefined_after_fork
         },
         % create a forked replica and respond to client
         ForkedPid = spawn(ForkNode, fun() -> loop(ForkedState) end),
         Client ! {Ref, ForkedPid},
         loop(State);

      % Change this replica's configuration
      {Ref, Client, reconfigure, NewConf=#conf{pids=NewReplicas}} ->
         Client ! {Ref, ok},
         Self = self(),
         case lists:member(Self, NewReplicas) of
            true ->
               loop(State#state{conf = NewConf});
            false ->
               Core:stop(reconfiguration)
         end;

      % Return the configuration at this replica
      {Ref, Client, get_conf} ->
         Client ! {Ref, Conf},
         loop(State);

      % Stop this replica
      {Ref, Client, stop, Reason} ->
         Client ! {Ref, Core:stop(Reason)};

      % Unexpected message
      UnexpectedMessage ->
         io:format("Received unexpected message ~p at ~p (~p)\n",
            [UnexpectedMessage, self(), ?MODULE])
   end.


% Given a list of responses from quorum replicas, return the response that is
% tagged with the largest number of updates.
maxResponse([ Head | Tail ]) ->
   maxResponse(Head, Tail).

maxResponse({_Count, Response}, []) ->
   Response;
maxResponse({Count, _Response}, [Head = {Count1, _} | Tail]) when Count1 > Count ->
   maxResponse(Head, Tail);
maxResponse(Max, [_Head | Tail]) ->
   maxResponse(Max, Tail).
