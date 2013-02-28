-module(chain).
-behaviour(ldsm).
-include("magic_numbers.hrl").
-compile({inline, [handle_msg/4]}).

-export([
      new_from/4
   ]).

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
      init_sm/1,
      handle_cmd/3,
      is_mutating/1,
      stop/2,
      export/1,
      import/1
   ]).

% Server callbacks
-export([
      init/2,
      handle_msg/3
   ]).

-include("helper_macros.hrl").
-include("libdist.hrl").

-record(state, {
      me,
      sm,
      conf,
      previous,
      next,
      unstable,
      stable_count = 0,
      next_cmd_num = 0
   }).


new_from(Pid, _ChainArgs, Nodes, Retry) ->
   Module = libdist_utils:call(Pid, get_sm_module, ?TO),
   % spawn new replicas
   Replicas = [ server:start(N, ?MODULE, no_core) || N <- Nodes ],
   % create a configuration and inform all the replicas of it
   Conf0 = #rconf{protocol = ?MODULE, args = Module, version = 0},
   {ok, Conf} = reconfigure(Conf0, Replicas, [], Retry),
   NewRootConf = libdist_utils:call(Pid, {replace, Pid, Conf}, Retry),
   libdist_utils:multicall(Replicas, {inherit_sm, Pid, ?TO}, Retry),
   NewRootConf.


% Create a new chain replicated state machine
new(SMSettings = {Module, _}, ChainArgs, Nodes, Timeout) ->
   % spawn new replicas
   Replicas = [ new_replica(N, SMSettings, ChainArgs) || N <- Nodes ],
   % create a configuration and inform all the replicas of it
   Conf0 = #rconf{protocol = ?MODULE, args = Module, version = 0},
   reconfigure(Conf0, Replicas, [], Timeout).   % return includes configuration


% Start a new replica
new_replica(Node, SMSettings, _RepArgs) ->
   server:start(Node, ?MODULE, {new, SMSettings}).


% Send an asynchronous command to a replicated object
cast(_Obj=#rconf{pids = Pids = [Head | _], args = SMModule}, Command) ->
   Target = case SMModule:is_mutating(Command) of
      true -> Head;
      false -> lists:last(Pids)
   end,
   libdist_utils:cast(Target, {command, Command}).


% Send a synchronous command to a replicated object
call(Obj, Command, Timeout) ->
   libdist_utils:collect(cast(Obj, Command), Timeout).


% Reconfigure the replicated object with a new set of replicas
reconfigure(OldConf, NewReplicas, _NewArgs, Timeout) ->
   #rconf{version = Vn, pids = OldReplicas} = OldConf,
   NewConf = OldConf#rconf{ version = Vn + 1, pids = NewReplicas },
   % This takes out the replicas in the old configuration but not in the new one
   Refs = libdist_utils:multicast(OldReplicas, {reconfigure, NewConf}),
   case libdist_utils:collectall(Refs, Timeout) of
      {ok, _} ->
         % This integrates replicas in the new configuration that are not old
         Refs2 = libdist_utils:multicast(NewReplicas, {reconfigure, NewConf}),
         case libdist_utils:collectall(Refs2, Timeout) of
            {ok, _} ->
               {ok, NewConf};
            Error ->
               Error
         end;
      % TODO: distinguish which case of timeout occurred
      Error ->
         Error
   end.


% Stop one of the replicas of the replicated object.
stop(Obj=#rconf{version = Vn, pids = OldReplicas}, N, Reason, Timeout) ->
   Pid = lists:nth(N, OldReplicas),
   case libdist_utils:collect(libdist_utils:cast(Pid, {stop, Reason}), Timeout) of
      {ok, _} ->
         NewReplicas = lists:delete(Pid, OldReplicas),
         NewConf = Obj#rconf{version = Vn + 1, pids = NewReplicas},
         Refs = libdist_utils:multicast(NewReplicas, {reconfigure, NewConf}),
         case libdist_utils:collectall(Refs, Timeout) of
            {ok, _} ->
               {ok, NewConf};
            Error ->
               Error
         end;
      % TODO: distinguish which case of timeout occurred
      Error ->
         Error
   end.



%%%%%%%%%%%%%%%%%%%%%%%%%%%
% State Machine Callbacks %
%%%%%%%%%%%%%%%%%%%%%%%%%%%


init_sm({SMModule, SMArgs}) ->
   init([], {SMModule, SMArgs}).

handle_cmd(State = #state{me = Me}, Message, AllowSideEffects) ->
   case handle_msg(Me, Message, AllowSideEffects, State) of
      {_, NewState} -> {noreply, NewState};
      _ -> noreply
   end.

is_mutating(_) ->
   true.

stop(#state{sm= SM}, Reason) ->
   ldsm:stop(SM, Reason).

export(State = #state{sm = SM, unstable = Unstable}) ->
   State#state{
      sm = ldsm:export(SM),
      unstable = ets:tab2list(Unstable)
   }.

import(State = #state{sm = ExportedSM, unstable = UnstableList}) ->
   Unstable = ets:new(unstable_commands, []),
   ets:insert(Unstable, UnstableList),
   State#state{
      sm = ldsm:import(ExportedSM),
      unstable = Unstable
   }.


%%%%%%%%%%%%%%%%%%%%
% Server Callbacks %
%%%%%%%%%%%%%%%%%%%%


% Initialize the state of a new replica
init(Me, no_core) ->
   #state{
      me = Me,
      conf = #rconf{protocol = ?MODULE},
      unstable = ets:new(unstable_commands, [])
   };

init(Me, {new, {SMModule, SMArgs}}) ->
   #state{
      me = Me,
      sm = ldsm:start(SMModule, SMArgs),
      conf = #rconf{protocol = ?MODULE},
      unstable = ets:new(unstable_commands, [])
   }.


% Handle a queued message
handle_msg(Me, Message, State) ->
   handle_msg(Me, Message, true, State).


% Handle a queued message
handle_msg(Me, Message, AllowSideEffects, State = #state{
      sm = SM,
      conf = Conf,
      previous = Prev,
      next = Next,
      unstable = Unstable,
      stable_count = StableCount,
      next_cmd_num = NextCmdNum
   }) ->
   ASE = ((Next == chain_tail) and AllowSideEffects),
   case Message of
      % Handle command as the HEAD of the chain
      {Ref, Client, {command, Command}} when Prev == chain_head ->
         ets:insert(Unstable, {NextCmdNum, Ref, Client, Command}),
         ?SEND(Next, {Ref, Client, command, NextCmdNum, Command}),
         {consume, State#state{next_cmd_num = NextCmdNum + 1}};

      % Handle command as any replica in the MIDDLE of the chain
      {Ref, Client, command, NextCmdNum, Cmd} = Msg when Next /= chain_tail ->
         ?SEND(Next, Msg),
         ets:insert(Unstable, {NextCmdNum, Ref, Client, Cmd}),
         {consume, State#state{next_cmd_num = NextCmdNum + 1}};

      % Handle update command as the TAIL of the chain
      {Ref, Client, command, NextCmdNum, Command} ->
         Result = ldsm:do(SM, Command, ASE),
         ?REPLY(Client, {Ref, Result}, ASE, SM),
         ?SEND(Prev, {stabilized, NextCmdNum}),
         NextCount = NextCmdNum + 1,
         {consume, State#state{next_cmd_num=NextCount, stable_count=NextCount}};

      % Handle query command as the TAIL of the chain
      {Ref, Client, {command, Command}} ->
         Result = ldsm:do(SM, Command, ASE),
         ?REPLY(Client, {Ref, Result}, ASE, SM),
         consume;

      {stabilized, StableCount} = Msg ->
         case ets:lookup(Unstable, StableCount) of
            [{StableCount, _Ref, _Client, Command}] ->
               ldsm:do(SM, Command, ASE),
               if
                  Prev /= chain_head -> ?SEND(Prev, Msg);
                  true -> do_not_forward
               end,
               ets:delete(Unstable, StableCount),
               {consume, State#state{stable_count=StableCount+1}};

            Other ->
               io:format("Unexpected result when stabilizing at chain replica:
                  ~p\n", [Other]),
               exit(error_stabilizing_at_chain_node)
         end;

      % Change this replica's configuration
      % TODO: handle reconfiguration in nested protocols
      {Ref, Client, {reconfigure, NewConf=#rconf{pids=NewReplicas}}} ->
         Client ! {Ref, ok},
         case lists:member(Me, NewReplicas) of
            true ->
               {_, NewPrev, NewNext} = libdist_utils:ipn(Me, NewReplicas),
               {consume, State#state{
                     conf = NewConf,
                     previous = NewPrev,
                     next = NewNext
                  }};
            false ->
               ldsm:stop(SM,reconfigure),
               {stop, reconfigure}
         end;


      % update the configuration by replacing OldReplica with NewReplica.
      {Ref, Client, {replace, OldReplica, NewReplica}} ->
         % update configuration trees locally and compute the new root conf
         {NewState, NewRootConf} = replace_replica(State, OldReplica, NewReplica),
         Client ! {Ref, NewRootConf},
         {consume, NewState};

      % update the configuration by replacing OldReplica with NewReplica
      {replace, OldReplica, NewReplica} ->
         {consume, replace_replica(State, OldReplica, NewReplica)};

      % Stop this replica
      {Ref, Client, {stop, Reason}} ->
         ?REPLY(Client, {Ref, ldsm:stop(SM, Reason)}, ASE, SM),
         {stop, Reason};

      % Return the configuration at this replica
      {Ref, Client, get_conf} ->
         Client ! {Ref, Conf},
         consume;

      {Ref, Client, {inherit_sm, Pid, Retry}} ->
         NewSM = ldsm:import(libdist_utils:call(Pid, get_sm, Retry)),
         Client ! {Ref, ok},
         {consume, State#state{sm = NewSM}};

      % Return the state machine's module
      {Ref, Client, get_sm_module} ->
         Client ! {Ref, ?MODULE},
         consume;

      % Return the state machine's state
      % TODO: change this into some sort of background state transfer
      {Ref, Client, get_sm} ->
         Client ! {Ref, ldsm:export(?MODULE, export(State))},
         consume;

      _ ->
         no_match
   end.



%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%


replace_replica(State = #state{
      me = Me,
      sm = SM,
      conf = Conf = #rconf{version = Vn, pids = Replicas}
   }, OldReplica, NewReplica) ->

   % if needed, notify siblings in RP Tree of configuration change. This is
   % only triggered by the actual process being replaced and nobody else
   case Me == OldReplica of
      true ->
         % trigger replacement/reconfiguration on other replicas
         Others = lists:delete(OldReplica, Replicas),
         [ ?SEND(R, {replace, OldReplica, NewReplica}) || R <- Others ],

         NewReps = libdist_utils:list_replace(OldReplica, NewReplica, Replicas),
         NewConf = Conf#rconf{version = Vn+1, pids = NewReps},

         % update higher levels of the RP-Tree if they exist
         NewRootConf = case ldsm:is_rp_protocol(SM) of
            false -> NewConf;
            true -> replace_replica(SM:state(), Conf, NewConf)
         end,

         % create the new state and configuration
         {_, NewPrev, NewNext} = libdist_utils:ipn(NewReplica, NewReps),
         NewState = State#state{
            me = NewReplica,
            conf = NewConf,
            next = NewNext,
            previous = NewPrev
         },

         % return the new state and the new root configuration
         {NewState, NewRootConf};

      false ->
         % modify the configuration and return the updated state
         NewReps = libdist_utils:list_replace(OldReplica, NewReplica, Replicas),
         {_, NewPrev, NewNext} = libdist_utils:ipn(Me, NewReps),
         State#state{
            conf = Conf#rconf{version = Vn+1, pids = NewReps},
            next = NewNext,
            previous = NewPrev
         }
   end.

