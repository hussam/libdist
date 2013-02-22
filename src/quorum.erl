-module(quorum).
-compile({inline, [handle_msg/4]}).

% Repobj interface
-export([
      new/4,
      new_replica/3,
      do/3,
      reconfigure/4,
      stop/4
   ]).

% State machine interface
-export([
      new/1,
      handle_cmd/3,
      is_mutating/1,
      stop/2
   ]).

% Server callbacks
-export([
      init/2,
      handle_msg/3
   ]).

-include("helper_macros.hrl").
-include("repobj.hrl").

-record(state, {
      me,
      core,
      conf,
      others = [],
      updates_count = 0
   }).

% Create a new chain replicated state machine
new(CoreSettings = {Module, _}, QArgs, Nodes, Retry) ->
   % spawn new replicas
   Replicas = [ new_replica(N, CoreSettings, QArgs) || N <- Nodes ],

   % compute the configuration arguments for quorum intersection
   Args = make_conf_args(length(Replicas), QArgs),

   % create a configuration and inform all the replicas of it
   Conf0 = #rconf{protocol = ?MODULE, args = {Module, Args}, version = 0},
   reconfigure(Conf0, Replicas, [], Retry).   % returns the new configuration


% Start a new replica
new_replica(Node, CoreSettings, _RepArgs) ->
   server:start(Node, ?MODULE, CoreSettings).

% Send a command to a replicated object
do(#rconf{pids = Replicas, args = {CoreModule, Args}}, Command, Retry) ->
   Targets = case proplists:get_bool(shuffle, Args) of
      true -> shuffle(Replicas);
      false -> Replicas
   end,

   {QName, QSize} = case CoreModule:is_mutating(Command) of
      true -> {w, proplists:get_value(w, Args)};
      false -> {r, proplists:get_value(r, Args)}
   end,

   maxResponse([ Response || {_Pid, Response} <-
         libdist_utils:multicall(Targets, QName, Command, QSize, Retry) ]).

% Reconfigure the replicated object with a new set of replicas
reconfigure(OldConf, NewPids, NewArgs, Retry) ->
   #rconf{version = OldVn, pids = OldPids, args = {CMod, OldArgs}} = OldConf,
   % the new configuration
   NewConf = OldConf#rconf{
      version = OldVn + 1,
      pids = NewPids,
      args = {CMod, remake_conf_args(length(NewPids), NewArgs, OldArgs)}
   },
   % This takes out the replicas in the old configuration but not in the new one
   libdist_utils:multicall(OldPids, reconfigure, NewConf, Retry),
   % This integrates the replicas in the new configuration that are not old
   libdist_utils:multicall(NewPids, reconfigure, NewConf, Retry),
   NewConf.    % return the new configuration

% Stop one of the replicas of the replicated object.
stop(Obj=#rconf{version = Vn, pids = OldReplicas}, N, Reason, Retry) ->
   Pid = lists:nth(N, OldReplicas),
   libdist_utils:call(Pid, stop, Reason, Retry),
   NewReplicas = lists:delete(Pid, OldReplicas),
   NewConf = Obj#rconf{version = Vn + 1, pids = NewReplicas},
   libdist_utils:multicall(NewReplicas, reconfigure, NewConf, Retry),
   NewConf.


%%%%%%%%%%%%%%%%%%%%%%%%%%%
% State Machine Callbacks %
%%%%%%%%%%%%%%%%%%%%%%%%%%%


new({CoreModule, CoreArgs}) ->
   init([], {CoreModule, CoreArgs}).

handle_cmd(State = #state{me = Me}, AllowSideEffects, Message) ->
   handle_msg(Me, Message, AllowSideEffects, State).

is_mutating(_) ->
   true.

stop(#state{core = Core}, Reason) ->
   Core:stop(Reason).


%%%%%%%%%%%%%%%%%%%%
% Server Callbacks %
%%%%%%%%%%%%%%%%%%%%


% Initialize the state of a new replica
init(Me, {CoreModule, CoreArgs}) ->
   #state{
      me = Me,
      core = libdist_sm:new(CoreModule, CoreArgs),
      conf = #rconf{protocol = ?MODULE}
   }.


% Handle a queued message
handle_msg(Me, Message, State) ->
   handle_msg(Me, Message, true, State).


% Handle a queued message
handle_msg(Me, Message, AllowSideEffects, State = #state{
      core = Core,
      conf = Conf,
      updates_count = UpdatesCount
   }) ->
   case Message of
      % Respond to a command as a member of a read quorum
      {Ref, Coordinator, r, Command} ->
         Coordinator ! {Ref, {UpdatesCount, Core:do(AllowSideEffects, Command)}},
         consume;

      % Respond to a command as a member of a write quorum
      {Ref, Coordinator, w, Command} ->
         NewCount = UpdatesCount + 1,
         Coordinator ! {Ref, {NewCount, Core:do(AllowSideEffects, Command)}},
         {consume, State#state{updates_count = NewCount}};

      % Change this replica's configuration
      % TODO: handle reconfiguration in nested protocols
      {Ref, Client, reconfigure, NewConf=#rconf{pids=NewReplicas}} ->
         ?ECS({Ref, ok}, AllowSideEffects, Client),
         case lists:member(Me, NewReplicas) of
            true ->
               {consume, State#state{conf = NewConf}};
            false ->
               Core:stop(reconfiguration),
               {stop, reconfiguration}
         end;

      % Return the configuration at this replica
      {Ref, Client, get_conf} ->
         ?ECS({Ref, Conf}, AllowSideEffects, Client),
         consume;

      % Stop this replica
      {Ref, Client, stop, Reason} ->
         ?ECS({Ref, Core:stop(Reason)}, AllowSideEffects, Client),
         {stop, Reason};

      _ ->
         no_match
   end.


%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%


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


shuffle(List) ->
   %% Determine the log n portion then randomize the list.
   randomize(round(math:log(length(List)) + 0.5), List).

randomize(1, List) ->
   randomize(List);
randomize(T, List) ->
   lists:foldl(
      fun(_E, Acc) -> randomize(Acc) end,
      randomize(List),
      lists:seq(1, T - 1)
   ).

randomize(List) ->
   D = lists:map(fun(A) -> {random:uniform(), A} end, List),
   {_, D1} = lists:unzip(lists:keysort(1, D)),
   D1.


make_conf_args(N, QArgs) ->
   R = case proplists:lookup(r, QArgs) of
      {r, ReadQuorumSize} -> ReadQuorumSize;
      none -> trunc(N/2) + 1
   end,
   W = case proplists:lookup(w, QArgs) of
      {w, WriteQuorumSize} -> WriteQuorumSize;
      none -> trunc(N/2) + 1
   end,
   Shuffle = proplists:get_bool(shuffle, QArgs),

   [{r, R}, {w, W}, {shuffle, Shuffle}].

remake_conf_args(NewN, NewArgs, OldConfArgs) ->
   OldR = proplists:get_value(r, OldConfArgs),
   OldW = proplists:get_value(w, OldConfArgs),
   OldShuffle = proplists:get_bool(shuffle, OldConfArgs),

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
   NewShuffle = case proplists:lookup(shuffle, NewArgs) of
      {shuffle, S} -> S;
      none -> OldShuffle
   end,

   [{r, NewR}, {w, NewW}, {shuffle, NewShuffle}].

