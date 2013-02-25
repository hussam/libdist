-module(quorum).
-compile({inline, [handle_msg/4]}).

% Repobj interface
-export([
      new/4,
      new_replica/3,
      cast/2,
      call/3,
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

% Other functions
-export([
      max_response/1
   ]).

-include("helper_macros.hrl").
-include("libdist.hrl").

-record(state, {
      me,
      core,
      conf,
      others = [],
      updates_count = 0
   }).

% Create a new chain replicated state machine
new(CoreSettings = {Module, _}, QArgs, Nodes, Timeout) ->
   % spawn new replicas
   Replicas = [ new_replica(N, CoreSettings, QArgs) || N <- Nodes ],

   % compute the configuration arguments for quorum intersection
   Args = make_conf_args(length(Replicas), QArgs),

   % create a configuration and inform all the replicas of it
   Conf0 = #rconf{protocol = ?MODULE, args = {Module, Args}, version = 0},
   reconfigure(Conf0, Replicas, [], Timeout).   % return includes configuration


% Start a new replica
new_replica(Node, CoreSettings, _RepArgs) ->
   server:start(Node, ?MODULE, CoreSettings).

% Send an asynchronous command to a replicated object
cast(#rconf{pids = Replicas, args = {CoreModule, Args}}, Command) ->
   Targets = case proplists:get_bool(shuffle, Args) of
      true -> shuffle(Replicas);
      false -> Replicas
   end,

   QName = case CoreModule:is_mutating(Command) of
      true -> w;
      false -> r
   end,

   % XXX: user must watch out when collecting to choose max response
   libdist_utils:multicast(Targets, QName, Command).


% Send a synchronous command to a replicated object
call(#rconf{pids = Replicas, args = {CoreModule, Args}}, Command, Timeout) ->
   Targets = case proplists:get_bool(shuffle, Args) of
      true -> shuffle(Replicas);
      false -> Replicas
   end,

   {QName, QSize} = case CoreModule:is_mutating(Command) of
      true -> {w, proplists:get_value(w, Args)};
      false -> {r, proplists:get_value(r, Args)}
   end,

   Refs = libdist_utils:multicast(Targets, QName, Command),
   case libdist_utils:collectmany(Refs, QSize, Timeout) of
      {ok, Responses} ->
         {ok, max_response([ Resp || {_Pid, Resp} <- Responses ])};
      Error ->
         Error
   end.



% Reconfigure the replicated object with a new set of replicas
reconfigure(OldConf, NewPids, NewArgs, Timeout) ->
   #rconf{version = OldVn, pids = OldPids, args = {CMod, OldArgs}} = OldConf,
   % the new configuration
   NewConf = OldConf#rconf{
      version = OldVn + 1,
      pids = NewPids,
      args = {CMod, remake_conf_args(length(NewPids), NewArgs, OldArgs)}
   },
   % This takes out replicas in the old configuration but not in the new one
   Refs = libdist_utils:multicast(OldPids, reconfigure, NewConf),
   case libdist_utils:collectall(Refs, Timeout) of
      {ok, _} ->
         % This integrates replicas in the new configuration that are not old
         Refs2 = libdist_utils:multicast(NewPids, reconfigure, NewConf),
         case libdist_utils:collectall(Refs2, Timeout) of
            {ok, _} ->
               {ok, NewConf};    % return the new configuration
            Error ->
               Error
         end;
      Error ->
         Error
   end.


% Stop one of the replicas of the replicated object.
stop(Obj=#rconf{version = Vn, pids = OldReplicas}, N, Reason, Timeout) ->
   Pid = lists:nth(N, OldReplicas),
   case libdist_utils:collect(libdist_utils:cast(Pid, stop, Reason), Timeout) of
      {ok, _} ->
         NewReplicas = lists:delete(Pid, OldReplicas),
         NewConf = Obj#rconf{version = Vn + 1, pids = NewReplicas},
         Refs = libdist_utils:multicast(NewReplicas, reconfigure, NewConf),
         case libdist_utils:collectall(Refs, Timeout) of
            {ok, _} ->
               {ok, NewConf};
            Error ->
               Error
         end;
      Error ->
         Error
   end.


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


% Given a list of responses from quorum replicas, return the response that is
% tagged with the largest number of updates.
max_response([ Head | Tail ]) ->
   maxResponse(Head, Tail).



%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%


% Given a list of responses from quorum replicas, return the response that is
% tagged with the largest number of updates.
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

