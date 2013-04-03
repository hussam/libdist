-module(cache).
-behaviour(replica).

% replica callbacks
-export([
      type/0,
      conf_args/1,
      cast/3,
      init_replica/1,
      import/1,
      export/1,
      export/2,
      update_state/3,
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
cast(#conf{replicas = [Head | Tail], sm_mod = SMModule}, RId, Command) ->
   {Tag, Target} = case SMModule:is_mutating(Command) of
      true -> {write, Head};
      false -> {read, lists:nth(random:uniform(length(Tail)), Tail)}
   end,
   libdist_utils:cast(Target, RId, {Tag, Command}).


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

% Export part of a cache replica's state
export(State, _NewTag) ->
   export(State).    % we are ignoring the tag here. This could be changed


% Update the protocol's custom state (due to replacement or reconfiguration)
update_state(_Me, #conf{replicas = [Backend | Caches]}, State) ->
   State#cache_state{backend = Backend, caches = Caches}.


% Handle a queued message
handle_msg(Me, Message, ASE = _AllowSideEffects, SM, _State = #cache_state{
      local_store = LocalStore,
      backend = Backend,
      caches = Caches
   }) ->
   case Message of
      % Handle a read command as a cache replica
      {Ref, Client, RId, {read, Command}} ->
         case ets:lookup(LocalStore, Command) of
            [{_, Result}] ->     % cache hit
               ?SEND(Client, RId, {Ref, Result}, ASE);
            _ ->                 % cache miss
               ?SEND(Backend, RId, {fwd, Ref, Client, RId, Command, Me}, ASE)
         end,
         consume;

      % Handle a write command as a backend replica
      {Ref, Client, RId, {write, Command}} ->
         ldsm:do(SM, Ref, Client, Command, ASE),
         [ ?SEND(C, RId, {invalidate, Command}, ASE) || C <- Caches ],
         consume;

      % Handle a command forwarded by a cache replica
      {fwd, Ref, Client, RId, Command, CacheReplica} ->
         Result = ldsm:do(SM, Command, ASE),
         ?SEND(Client, RId, {Ref, Result}, ASE),
         ?SEND(CacheReplica, RId, {cache_update, Command, Result}, ASE),
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
