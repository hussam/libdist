-module(shard).
-behaviour(replica).

% replica callbacks
-export([
      type/0,
      conf_args/1,
      cast/2,
      init_replica/1,
      import/1,
      export/1,
      export/2,
      update_state/3,
      handle_failure/5,
      handle_msg/5
   ]).

-include("constants.hrl").
-include("helper_macros.hrl").
-include("libdist.hrl").

-define(HEAD, '$shard_head').


%%%%%%%%%%%%%%%%%%%%%
% Replica Callbacks %
%%%%%%%%%%%%%%%%%%%%%


% This is a partitioning protocol and it does not make use of extra arguments
type() -> ?PART.
conf_args(Args) -> Args.


% Send an asynchronous command to a sharded object
cast(#conf{partitions = Ps, route_fn = F}, Command) ->
   {_, Target} = F(Command, Ps),
   libdist_utils:cast(Target, {command, Command}).

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

% Handle the failure of a shard
handle_failure(_Me, _NewConf, State, {_FailedTag, _FailedPid}, _Info) ->
   State.   % FIXME: implement this properly!

% Handle a queued message
handle_msg(_Me, Message, ASE = _AllowSideEffects, SM, _State) ->
   case Message of
      {Ref, Client, {command, Command}} ->
         ldsm:do(SM, Ref, Client, Command, ASE),
         consume;

      {Ref, Client, {broadcast, Command}} ->
         % This was part of a broadcast, do not allow side effects
         ldsm:do(SM, Ref, Client, Command, false),
         consume;

      _ ->
         no_match
   end.


%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%

