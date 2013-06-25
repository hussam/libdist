-module(cache).
-behaviour(replica).

% replica callbacks
-export([
      type/0,
      conf_args/1,
      cast/2,
      init_replica/1,
      import/1,
      export/1,
      update_state/3,
      handle_failure/5,
      handle_msg/5
   ]).

-include("constants.hrl").
-include("helper_macros.hrl").
-include("libdist.hrl").

% State specific to a chain replica
-record(cache_state, {
      local_store,
      backend,
      caches = []
   }).

-define(ETS_OPTS, []).

%%%%%%%%%%%%%%%%%%%%%
% Replica Callbacks %
%%%%%%%%%%%%%%%%%%%%%


% This is a partitioning protocol and it does not make use of extra arguments
type() -> ?REPL.
conf_args(Args) -> Args.


% Send an asynchronous command to a chain replicated object
cast(#conf{replicas = [Head | Tail], sm_mod = SMModule}, Command) ->
   {Tag, Target} = case SMModule:is_mutating(Command) of
      true -> {write, Head};
      false -> {read, lists:nth(random:uniform(length(Tail)), Tail)}
   end,
   libdist_utils:cast(Target, {Tag, Command}).


% Initialize the state of a new replica
init_replica(_Me) ->
   #cache_state{
      local_store = ets:new(cached_commands, ?ETS_OPTS)
   }.


% Import a previously exported cache state
import(_) ->
   #cache_state{
      local_store = ets:new(cached_commands, ?ETS_OPTS)
   }.


% Export a cache replica state
export(_) -> [].     % do not export anything


% Update the protocol's custom state (due to replacement or reconfiguration)
update_state(_Me, #conf{replicas = [Backend | Caches]}, State) ->
   State#cache_state{backend = Backend, caches = Caches}.


% Handle the failure of a cache or a replica
handle_failure(Me, NewConf, State=#cache_state{backend=Backend}, FailedPid, _Info) ->
   case FailedPid of
      Backend ->
         % do not update the state. This will result in all write requests
         % timing out sent to the backend timing out, and caches can still
         % service read requests.
         State;

      _ ->  % when a cache replica fails, just remove it.
         update_state(Me, NewConf, State)
   end.


% Handle a queued message
handle_msg(Me, Message, ASE = _AllowSideEffects, SM, _State = #cache_state{
      local_store = LocalStore,
      backend = Backend,
      caches = Caches
   }) ->
   case Message of
      % Handle a read command as a cache replica
      {Ref, Client, {read, Command}} ->
         case ets:lookup(LocalStore, Command) of
            [{_, Result}] ->     % cache hit
               ?SEND(Client, {Ref, Result}, ASE);
            _ ->                 % cache miss
               ?SEND(Backend, {fwd, Ref, Client, Command, Me}, ASE)
         end,
         consume;

      % Handle a write command as a backend replica
      {Ref, Client, {write, Command}} when Me == Backend ->
         ldsm:do(SM, Ref, Client, Command, ASE),
         [ ?SEND(C, {invalidate, Command}, ASE) || C <- Caches ],
         consume;

      % Handle a command forwarded by a cache replica
      {fwd, Ref, Client, Command, CacheReplica} ->
         Result = ldsm:do(SM, Command, ASE),
         ?SEND(Client, {Ref, Result}, ASE),
         ?SEND(CacheReplica, {cache_update, Command, Result}, ASE),
         consume;

      % Handle a cache update at a cache replica
      {cache_update, Command, Result} ->
         ets:insert(LocalStore, {Command, Result}),
         consume;

      % Handle a cache invalidation command at a cache replica
      {invalidate, Command} ->
         ets:delete(LocalStore, Command),
         consume;

      _ ->
         no_match
   end.
