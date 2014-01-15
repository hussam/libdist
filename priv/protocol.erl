-module({{modid}}).
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

% State specific to a {{modid}} replica
-record({{modid}}_state, {
      unstable,
      next_cmd_num = 0
   }).


%%%%%%%%%%%%%%%%%%%%%
% Replica Callbacks %
%%%%%%%%%%%%%%%%%%%%%


% This is a replication protocol and it does not make use of extra arguments
% and it does not overload any generic repobj functions
type() -> ?REPL.
conf_args(Args) -> Args.
overloads(_) -> false.


% Send an asynchronous command to a {{modid}} replicated object
cast(#conf{replicas = [Hd | _], sm_mod = SMModule}, Command) ->
   Tag = case SMModule:is_mutating(Command) of
      true -> write;
      false -> read
   end,
   libdist_utils:cast(Hd, {Tag, Command}).


% Initialize the state of a new replica
init_replica(_Me, _Args) ->
   #{{modid}}_state{
      unstable = ets:new(unstable_commands, [])
   }.


% Import a previously exported protocol state
import(ExportedState = #{{modid}}_state{ unstable = UnstableList }) ->
   Unstable = ets:new(unstable_commands, []),
   ets:insert(Unstable, UnstableList),
   ExportedState#{{modid}}_state{
      unstable = Unstable
   }.


% Export a {{modid}} replica state
export(State = #{{modid}}_state{unstable = Unstable}) ->
   State#{{modid}}_state{
      unstable = ets:tab2list(Unstable)
   }.


% Update the protocol's custom state (due to replacement or reconfiguration)
update_state(Me, #conf{replicas = _NewReps}, State) ->
   State#{{modid}}_state{ }.


% Handle the failure of a replica
handle_failure(Me, Conf = #conf{replicas = Pids}, State, FailedPid, _Info) ->
   NewConf = Conf#conf{
      replicas = lists:delete(FailedPid, Pids)
   },
   {NewConf, update_state(Me, NewConf, State)}.


% Handle a queued message
handle_msg(_Me, Message, ASE = _AllowSideEffects, SM, State = #{{modid}}_state{
      unstable = Unstable,
      next_cmd_num = NextCmdNum
   }) ->
   case Message of
      % Handle a write command
      {Ref, Client, {write, Command}} ->
         ldsm:do(SM, Ref, Client, Command, ASE),
         {consume, State#{{modid}}_state{next_cmd_num = NextCmdNum+ 1}};

      % Handle query command
      {Ref, Client, {read, Command}} ->
         ldsm:do(SM, Ref, Client, Command, ASE),
         consume;

      _ ->
         no_match
   end.
