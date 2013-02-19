-module(elastic_chain).
-export([
      activate/6
   ]).

% Server callbacks
-export([
      init/2,
      handle_msg/3
   ]).


-include("repobj.hrl").

-record(state, {
      core,
      conf,
      prev,
      next,
      last,
      unstable,
      stable_count = 0,
      next_cmd_num = 0
   }).


% Set/return the state of an active/non-immutable replica
activate(Me, Core, Conf=#rconf{pids = Replicas}, Unstable, StableCount, NextCmdNum) ->
   {_Index, Prev, Next} = libdist_utils:ipn(Me, Replicas),
   #state{
      core = Core,
      conf = Conf,
      prev = Prev,
      next = Next,
      unstable = Unstable,
      stable_count = StableCount,
      next_cmd_num = NextCmdNum
   }.


% Server callback should do nothing since the state should be initiated via the
% activate/6 method.
init(_, _) ->
   not_activated.


% State: ACTIVE
% When a replica is in an active state it can add commands to its history and
% respond to client requests. A replica stays in active state until it is wedged
handle_msg(_Me, Message, State = #state{
            core = Core,
            conf = Conf = #rconf{version = Vn},
            prev = Prev,
            next = Next,
            unstable = Unstable,
            stable_count = StableCount,
            next_cmd_num = NextCmdNum
   }) ->
   case Message of
      {_, _, read, {Vn, _}} = Msg when Next /= chain_tail ->
         Next ! Msg,
         consume;

      {Ref, Client, read, {Vn, Command}} ->  % Next == chain_tail
         Client ! {Ref, Core:do(Command)},
         consume;

      % Transition: add a command to stable history
      {Ref, Client, write, {Vn, Command}} when Prev == chain_head ->
         ets:insert(Unstable, {NextCmdNum, Ref, Client, Command}),
         Next ! {Ref, Client, Vn, write, NextCmdNum, Command},
         {consume, State#state{next_cmd_num = NextCmdNum + 1}};

      % XXX: we either trust clients to send commands to the head of the chain,
      % or we have to use some sort of message authentication mechanisms
      {Ref, Client, Vn, write, NextCmdNum, Command} = Msg when Next /= chain_tail ->
         Next ! Msg,    % Forward first as this shouldn't break semantics.
         ets:insert(Unstable, {NextCmdNum, Ref, Client, Command}),
         {consume, State#state{next_cmd_num = NextCmdNum + 1}};

      {Ref, Client, Vn, write, NextCmdNum, Command} ->    % Next == chain_tail
         Client ! {Ref, Core:do(Command)},
         Prev ! {Vn, stabilized, NextCmdNum},
         {consume, State#state{stable_count=StableCount+1,
               next_cmd_num=NextCmdNum+1}};

      {Vn, stabilized, StableCount} = Msg when Prev /= chain_head ->
         Command = ets:lookup_element(Unstable, StableCount, 4),
         Core:do(Command),    % XXX: should I apply first or forward first?
         Prev ! Msg,
         ets:delete(Unstable, StableCount),
         {consume, State#state{stable_count = StableCount + 1}};

      {Vn, stabilized, StableCount} ->    % Prev == chain_head
         Command = ets:lookup_element(Unstable, StableCount, 4),
         Core:do(Command),
         ets:delete(Unstable, StableCount),
         {consume, State#state{stable_count = StableCount + 1}};

      % Transition: wedgeState
      % take replica into immutable state
      {Ref, Client, wedge, Vn} ->
         Client ! {Ref, wedged},
         {consume, elastic:makeImmutable(Core, Conf, Unstable, StableCount,
               NextCmdNum)};


      % Return current configuration
      {Ref, Client, get_conf} ->
         Client ! {Ref, Conf},
         consume;

      % Stop this replica
      {Ref, Client, stop, Reason} ->
         Client ! {Ref, Core:stop(Reason)},
         {stop, Reason};


      % ignore other tagged messages and respond with current configuration
      {Ref, Client, _, _} ->
         Client ! {Ref, {error, {reconfigured, Conf}}},
         consume;

      % ignore everything else
      _ ->
         consume
   end;

handle_msg(_, _, not_activated) ->
   no_match.
