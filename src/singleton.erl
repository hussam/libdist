-module(singleton).
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
      handle_failure/5,
      handle_msg/5
   ]).

-include("constants.hrl").
-include("libdist.hrl").


%%%%%%%%%%%%%%%%%%%%%
% Replica Callbacks %
%%%%%%%%%%%%%%%%%%%%%


type() -> ?SINGLE.
conf_args(Args) -> Args.

% Send an asynchronous command to a singleton configuration
cast(#conf{replicas = [Pid]}, RId, Command) ->
   libdist_utils:cast(Pid, RId, {command, Command}).

% There is no singleton-specific state, so all these functions are meaningless
init_replica(_) -> [].
import(_) -> [].
export(_) -> [].
export(_,_) -> [].
update_state(_,_,_) -> [].
handle_failure(_, _, _, _, _) -> [].

% Handle a queued message
handle_msg(_Me, Message, ASE = _AllowSideEffects, SM, _State) ->
   case Message of
      % Handle a command for the core state machine
      {Ref, Client, _RId, {command, Command}} ->
         ldsm:do(SM, Ref, Client, Command, ASE),
         consume;
      _ ->
         no_match
   end.
