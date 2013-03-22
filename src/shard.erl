-module(shard).
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


%%%%%%%%%%%%%%%%%%%%%
% Replica Callbacks %
%%%%%%%%%%%%%%%%%%%%%


% This is a partitioning protocol and it does not make use of extra arguments
type() -> ?PART.
conf_args(Args) -> Args.


% Send an asynchronous command to a sharded object
cast(#conf{partitions = Ps, route_fn = F}, RId, Command) ->
   case RId of
      ?ALL ->
         Targets = [ Pid || {_, Pid} <- Ps ],
         {Ref, _} = libdist_utils:multicast(Targets, {command, Command}),
         Ref;
      _ ->
         {_, Target} = F(RId, Ps),
         libdist_utils:cast(Target, RId, {command, Command})
   end.

% Initialize the state of a new shard
init_replica(_Me) ->
   [].

% Import a previously exported shard state
import(_) ->
   [].

% Export a shard state
export(_) ->
   [].

% Export part of a shard's state
export(_, _) ->
   [].

% Update the protocol's custom state (due to replacement or reconfiguration)
update_state(_Me, _NewConf, _State) ->
   [].

% Handle a queued message
handle_msg(_Me, Message, ASE = _AllowSideEffects, SM, _State) ->
   case Message of
      {Ref, Client, _RId, {command, Command}} ->
         ldsm:do(SM, Ref, Client, Command, ASE),
         consume;

      _ ->
         no_match
   end.
