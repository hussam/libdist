-module(elastic_band).
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

-include("elastic.hrl").

-record(elastic_band_state, {
      predecessor,
      successor,
      timeout
   }).


%%%%%%%%%%%%%%%%%%%%%
% Replica Callbacks %
%%%%%%%%%%%%%%%%%%%%%


% This is a partitioning protocol and it does not make use of extra arguments
type() -> ?PART.
conf_args(Args) -> Args.
overloads(_) -> false.

% Send an asynchronous command to a sharded object
cast(#conf{partitions = Ps, route_fn = F}, Command) ->
   {_, Target} = F(Command, Ps),
   libdist_utils:cast(Target, {command, Command}).

% Initialize the state of a new shard
init_replica(_Me, {RouteFn, Args}) when is_function(RouteFn) ->
   #elastic_band_state{
      timeout = proplists:get_value(timeout, Args, ?E_TO)
   }.

% Import a previously exported shard state
import(ExportedState) ->
   ExportedState.

% Export a shard state
export(State) ->
   State.

% Update the protocol's custom state (due to replacement or reconfiguration)
update_state(Me, NewConf = #conf{partitions = Shards}, OldState) ->
   Peers = [ P || {_Tag, P} <- Shards ],
   {_, Prev, Next} = libdist_utils:ipn(Me, Peers),
   OldState#elastic_band_state{
      predecessor = case Prev of chain_head -> lists:last(Peers); X -> X end,
      successor = case Next of chain_tail -> hd(Peers); X -> X end
   }.

% Handle the failure of a shard
handle_failure(_Me, Conf, State, _FailedPid, _Info) ->
   {Conf, State}.   % FIXME: implement this properly!

% Handle a queued message
handle_msg(Me, Message, ASE = _AllowSideEffects, SM, State=#elastic_band_state{
      predecessor = Sequencer,
      successor = Successor,
      timeout = Timeout
   }) ->
   case Message of
      {Ref, Client, {command, Command}} ->
         ldsm:do(SM, Ref, Client, Command, ASE),
         consume;

      {reconfig_needed, Me, FailedPid, _FailureInfo} ->
         ?SEND(Sequencer, {reconfig_request, Me, FailedPid}, true),
         consume;

      {reconfig_request, Successor=#conf{version=OldVn, replicas=OldPids},
                         FailureSuspect} ->
         % XXX: Currently assumes that Successor is a replicated object
         % XXX: A fail-stop model is assumed. Failed processes are just removed
         % from successor configuration. FIXME!
         NewSuccessor = Successor#conf{
            version = OldVn + 1,
            replicas = lists:delete(FailureSuspect, OldPids)
         },
         % FIXME: handle case reconfiguration fails
         {ok, _} = repobj:reconfigure(Successor, NewSuccessor, Timeout),
         {consume, State#elastic_band_state{successor = NewSuccessor}};

      {reconfig_request, _, _} ->
         consume;    % probably a retransmission from an old config

      _ ->
         no_match
   end.


%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%

