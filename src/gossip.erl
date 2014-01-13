% A push-pull anti-entropy gossip protocol.
% All writes are ordered according to a shared log. Writes are guaranteed to
% execute in log-order, and can be sent to any replica. Reads return a
% consistent snapshot of the internal state machine's state (i.e. Sequential
% Consistency guarantee).
-module(gossip).
-behaviour(replica).

% replica callbacks
-export([
      type/0,
      overloads/1,
      conf_args/1,
      cast/2,
      init_replica/2,
      import/1,
      export/1,
      update_state/3,
      handle_failure/5,
      handle_msg/5
   ]).

-include("constants.hrl").
-include("helper_macros.hrl").
-include("libdist.hrl").

% State specific to a gossip replica
-record(gossip_state, {
      conf_version,
      peer_index,
      epoch = 0,
      counter = 0,
      gossip_counter = 0,
      peers = [],
      num_peers = 0,
      peers_logpos = dict:new(),
      peers_last_write = dict:new(),
      peers_last_gossip = dict:new(),
      unstable,
      log = [],
      log_pos = nil,
      local_writes = [],
      gossip_period,
      seal_period,
      cleanup_period
   }).


-define(GOSSIP_PERIOD, 100).    % 0.1 second default gossip period
-define(SEAL_PERIOD, 1000).     % seal gossip epochs every 1 seconds
-define(CLEANUP_PERIOD, 5000).  % clean log every 5 seconds

%%%%%%%%%%%%%%%%%%%%%
% Replica Callbacks %
%%%%%%%%%%%%%%%%%%%%%


% This is a replication protocol and it does not make use of extra arguments
% and it does not overload any generic repobj functions
type() -> ?REPL.
conf_args(Args) -> proplists:lookup_all(shuffle, Args).
overloads(_) -> false.


% Send an asynchronous command to a gossip replicated object
cast(#conf{replicas=Reps=[Hd | _], sm_mod = SMModule, args=GArgs}, Command) ->
   Target = case proplists:get_bool(shuffle, GArgs) of
      true -> lists:nth(random:uniform(length(Reps)), Reps);
      false -> Hd
   end,
   Tag = case SMModule:is_mutating(Command) of
      true -> write;
      false -> read
   end,
   libdist_utils:cast(Target, {Tag, Command}).


% Initialize the state of a new replica
init_replica(Me, Args) ->
   GossipPeriod = proplists:get_value(gossip_period, Args, ?GOSSIP_PERIOD),
   SealPeriod = proplists:get_value(seal_period, Args, ?SEAL_PERIOD),
   CleanupPeriod = proplists:get_value(cleanup_period, Args, ?CLEANUP_PERIOD),
   State = #gossip_state{
      log_pos = {1, 0, 1, -1},
      gossip_period = GossipPeriod,
      seal_period = SealPeriod,
      cleanup_period = CleanupPeriod,
      unstable = ets:new(unstable_commands, [])
   },
   ?SEND_AFTER(GossipPeriod, Me, do_gossip, true),
   ?SEND_AFTER(CleanupPeriod, Me, do_cleanup, true),
   State.


% Import a previously exported protocol state
import(ExportedState=#gossip_state{
      peers_last_write = PLWList,
      peers_logpos = PLPList,
      peers_last_gossip = PLGList,
      unstable = UnstableList
   }) ->
   Unstable = ets:new(unstable_commands, []),
   ets:insert(Unstable, UnstableList),
   ExportedState#gossip_state{
      peers_last_write = dict:from_list(PLWList),
      peers_logpos = dict:from_list(PLPList),
      peers_last_gossip = dict:from_list(PLGList),
      unstable = UnstableList
   }.


% Export a gossip replica state
export(State = #gossip_state{
      peers_logpos = PLP,
      peers_last_write = PLW,
      peers_last_gossip = PLG,
      unstable = U
   }) ->
   State#gossip_state{
      peers_logpos = dict:to_list(PLP),
      peers_last_write = dict:to_list(PLW),
      peers_last_gossip = dict:to_list(PLG),
      unstable = ets:tab2list(U)
   }.


% Update the protocol's custom state (due to replacement or reconfiguration)
update_state(Me, #conf{version = Vn, replicas = NewReps}, State) ->
   {PIndex, _, _} = libdist_utils:ipn(Me, NewReps),
   NewPeers = lists:delete(Me, NewReps),
   State#gossip_state{
      conf_version = Vn,
      peer_index = PIndex,
      epoch = 0,
      counter = 0,
      peers = NewPeers,
      num_peers = length(NewPeers)
   }.


% Handle the failure of a replica
handle_failure(Me, Conf=#conf{version=Vn, replicas=Pids}, State, FailedPid, _Info) ->
   % TODO FIXME: "The configuration version slots" need to be sealed. The
   % surviving processes should communicate to find out what is the last slot
   % logged by the failed process, and insert an artificial "seal conf version"
   % slot after that.
   NewConf = Conf#conf{
      replicas = lists:delete(FailedPid, Pids)
   },
   {NewConf, update_state(Me, NewConf, State)}.


% Handle a queued message
handle_msg(Me, Message, ASE = _AllowSideEffects, SM, State = #gossip_state{
      conf_version = Vn,
      peer_index = PIndex,
      epoch = Epoch,
      counter = Counter,

      gossip_counter = GossipCounter,

      peers = Peers,
      num_peers = NumPeers,
      peers_logpos = PeersLP,
      peers_last_write = PeersLW,
      peers_last_gossip = PeersLG,

      unstable = Unstable,
      local_writes = LocalWrites,
      log = Log,
      log_pos = LogPos,

      cleanup_period = CleanupPeriod,
      gossip_period = GossipPeriod,
      seal_period = SealPeriod
   }) ->
   case Message of
      % Execute as much of the command log as possible
      exec_if_possible ->
         case can_exec_next(LogPos, Log) of
            false ->
               case LocalWrites of
                  [Hd | _] when LogPos < Hd ->  % we have pending local writes
                     ?SEND_AFTER(SealPeriod, Me, {req_seal_epoch, element(2,
                              LogPos)}, true);
                  _ ->
                     do_nothing
               end,
               {consume, State};

            {true, NextPos} ->
               case ets:lookup_element(Unstable, NextPos, 2) of
                  {Ref, Client, {write, Command}} ->
                     ldsm:do(SM, Ref, Client, Command, ASE),
                     handle_msg(Me, Message, ASE, SM, State#gossip_state{
                           log_pos = NextPos
                        });

                  seal_epoch ->
                     TrueNextPos = case NextPos of
                        {V, E, I, _C} when I =< NumPeers -> {V, E, I + 1, -1};
                        {V, E, _I, _C} -> {V, E + 1, 1, -1}
                     end,
                     handle_msg(Me, Message, ASE, SM, State#gossip_state{
                           log_pos = TrueNextPos
                        })
               end
         end;

      % Seal an epoch and move to the next one
      {seal, Vn, SealedEpoch} ->
         case SealedEpoch >= Epoch of
            true ->
               % seal current epoch and all epochs until 'SealedEpoch'
               NextLog = lists:foldl(
                  fun(Slot, AccLog) ->
                        ets:insert(Unstable, {Slot, seal_epoch}),
                        log_insert(Slot, AccLog)
                        % TODO: update local writes
                  end,
                  Log,
                  [ {Vn, Epoch, PIndex, Counter} |
                     [{Vn, E, PIndex, 0} || E <- lists:seq(Epoch+1, SealedEpoch)]]
               ),

               handle_msg(Me, exec_if_possible, ASE, SM, State#gossip_state{
                     epoch = SealedEpoch + 1,
                     counter = 0,
                     log = NextLog
                  });
            false ->
               consume     % older message
         end;

      % Handle a write command
      {_Ref, _Client, {write, _Command}} ->
         Slot = {Vn, Epoch, PIndex, Counter},
         ets:insert(Unstable, {Slot, Message}),
         handle_msg(Me, exec_if_possible, ASE, SM, State#gossip_state{
               local_writes = [Slot | LocalWrites],
               log = log_insert(Slot, Log),
               counter = Counter + 1
            });

      % Handle query command
      {Ref, Client, {read, Command}} ->
         ldsm:do(SM, Ref, Client, Command, ASE),
         consume;

      % Seal trigger
      {req_seal_epoch, EpochToSeal} ->
         [ ?SEND(P, {seal, Vn, EpochToSeal}, ASE) || P <- Peers ],
         case EpochToSeal == Epoch of
            false ->    % already sealed this epoch locally
               consume;
            true ->
               Slot = {Vn, Epoch, PIndex, Counter},
               ets:insert(Unstable, {Slot, seal_epoch}),
               {consume, State#gossip_state{
                     log = log_insert(Slot, Log),
                     epoch = Epoch + 1,
                     counter = 0
                  }}
         end;

      % Cleanup trigger
      do_cleanup ->
         MinLogPos = lists:foldl(
            fun(Peer, Min) ->
                  case dict:find(Peer, PeersLP) of
                     {ok, PeerLogPos} when PeerLogPos < Min -> PeerLogPos;
                     {ok, _} -> Min;
                     error -> error
                  end
            end,
            LogPos,
            Peers
         ),
         NextState = case MinLogPos of
            error ->    % cannot truncate yet
               State;
            _ ->
               {Drop, NewLog} = lists:splitwith(fun(X) -> X < MinLogPos end, Log),
               NewLW = lists:takewhile(fun(X) -> X > MinLogPos end, LocalWrites),
               [ ets:delete(Unstable, Slot) || Slot <- Drop ],
               State#gossip_state{ log = NewLog , local_writes = NewLW }
         end,
         ?SEND_AFTER(CleanupPeriod, Me, do_cleanup, true),
         {consume, NextState};

      % Gossip trigger
      do_gossip ->
         Peer = lists:nth(random:uniform(NumPeers), Peers),
         PeerLastWrite = case dict:find(Peer, PeersLW) of
            {ok, PLW} -> PLW;
            _ -> nil
         end,
         ?SEND(Peer, {gossip, Me, GossipCounter, LogPos, PeerLastWrite}, ASE),
         ?SEND_AFTER(GossipPeriod, Me, do_gossip, true),
         {consume, State#gossip_state{gossip_counter = GossipCounter + 1}};

      % Handle peer pull request
      {pull, Peer, PeerLogPos} ->
         case get_next(PeerLogPos, Log) of
            no_match -> do_nothing;
            Slot -> ?SEND(Peer, {push, ets:lookup(Unstable, Slot)}, ASE)
         end,
         consume;

      % Handle peer push gossip
      {push, Peer, PeerIndex, GossipUpdates} ->
         PeerLastWrite = case dict:find(Peer, PeersLW) of
            {ok, PLW} -> PLW;
            _ -> nil
         end,
         {NextLog, NextPLW} = lists:foldl(
            fun(SU = {Slot, _Update}, Acc = {AccLog, PLW}) ->
                  case lists:member(Slot, AccLog) of
                     true ->  % old update, do not insert!
                        Acc;
                     false ->
                        ets:insert(Unstable, SU),
                        case Slot of
                           {_, _, PeerIndex, _} when Slot > PLW ->
                              {log_insert(Slot, AccLog), Slot};
                           _ ->
                              {log_insert(Slot, AccLog), PLW}
                        end
                  end
            end,
            {Log, PeerLastWrite},
            GossipUpdates
         ),
         handle_msg(Me, exec_if_possible, ASE, SM, State#gossip_state{
               log = NextLog,
               peers_last_write = dict:store(Peer, NextPLW, PeersLW)
            });

      % Handle peer gossip message
      {gossip, Peer, PeerGossipCounter, PeerLogPos, PeerMyLW} ->
         case dict:find(Peer, PeersLG) of
            {ok, PLG} when PLG >= PeerGossipCounter ->
               consume;    % ignore old message
            _ ->
               handle_gossip(Me, ASE, Peer, PeerLogPos, PeerMyLW, State),

               {consume, State#gossip_state{
                     peers_last_gossip = dict:store(Peer, PeerGossipCounter,
                        PeersLG),
                     peers_logpos = dict:store(Peer, PeerLogPos, PeersLP)
                  }}
         end;

      % Return local log information. Useful for debugging.
      {Ref, Client, get_log} ->
         Client ! {Ref, {LogPos, Log, LocalWrites}},
         consume;

      _ ->
         no_match
   end.


%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%


% Get the next element in the log (list in this implementation)
% XXX: ASSUMES LOG ENTRIES ARE NOT ATOMS
get_next(_Elem, []) ->
   no_match;
get_next(Elem, [Elem, Next | _]) ->
   Next;
get_next({V, E, I, -1}, [Next = {V,E,I,0} | _]) ->    % Special case
   Next;
get_next(Elem, [_ | Tail]) ->
   get_next(Elem, Tail).


% Insert an element in the right position in the log (according to sort order)
log_insert(Elem, []) ->
   [Elem];
log_insert(Elem, Log = [Hd | _]) when Elem < Hd ->
   [Elem | Log];
log_insert(Elem, [Hd | Tail]) ->
   [Hd | log_insert(Elem, Tail)].


% Can we execute the next element in the log given our current position LogPos?
% This basically checks if the next filled position in the log is strict
% successor of the current filled position. If not, it returns false.
can_exec_next(LogPos = {Vn, Epoch, PIndex, Num}, Log) ->
   case get_next(LogPos, Log) of
      {Vn, Epoch, PIndex, NextNum} = NextPos when NextNum == Num + 1 ->
         {true, NextPos};

      _ ->
         false
   end.


-compile({inline, [handle_gossip/6]}).
handle_gossip(Me, ASE, Peer, PeerLogPos, PeerMyLW, _State = #gossip_state{
      peer_index = PIndex,
      log = Log,
      log_pos = LogPos,
      unstable = Unstable,
      local_writes = LocalWrites
   }) ->
   % There are two potential updates that we can send the peer; one for
   % its next log position (assuming ours is greater) and one for our
   % local updates position (assuming they are lagging behind).

   % XXX: local updates position is only added to disseminate information
   % quickly. Log position updates are sufficient to make the algorithm
   % work. Local updates position allows us to send information that we
   % know for sure is fresher on our side (because they are local writes)
   % instead of sending a random (or even a sequence after the peer's
   % current) log position. A better approach could be something along the
   % lines of BitTorrent where the peer sends a hash of the log positions
   % it has and we compare it locally and request or send missing pieces.
   % TODO: come up with a better way to disseminate information

   LocalWritesUpdates = case LocalWrites of
      [ LastLocalWrite | _ ] when LastLocalWrite > PeerMyLW ->
         % XXX: creates a list and then traverses it. FIXME
         [
            lists:last(
               lists:takewhile(fun(X) -> X /= PeerMyLW end, LocalWrites))
         ];

      _ ->
         []
   end,
   GossipSlots = case PeerLogPos < LogPos of
      true -> ordsets:add_element(get_next(PeerLogPos, Log), LocalWritesUpdates);
      false -> LocalWritesUpdates
   end,
   case GossipSlots of
      [] ->
         do_nothing;
      _ ->
         GossipUpdates = lists:map(
            fun(S) -> {S, ets:lookup_element(Unstable, S, 2)} end,
            GossipSlots
         ),
         ?SEND(Peer, {push, Me, PIndex, GossipUpdates}, ASE)
   end,

   case PeerLogPos > LogPos of
      true -> ?SEND(Peer, {pull, Me, LogPos}, ASE);
      false -> do_nothing
   end.
