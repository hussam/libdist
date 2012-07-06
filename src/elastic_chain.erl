-module(elastic_chain).
-export([
      activeLoop/5
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

% State: ACTIVE
% When a replica is in an active state it can add commands to its history and
% respond to client requests. A replica stays in active state until it is wedged
activeLoop(Core, Conf=#conf{pids = Replicas}, Unstable, StableCount, NextCmdNum) ->
   {_Index, Prev, Next} = repobj_utils:ipn(self(), Replicas),
   State = #state{
      core = Core,
      conf = Conf,
      prev = Prev,
      next = Next,
      unstable = Unstable,
      stable_count = StableCount,
      next_cmd_num = NextCmdNum
   },
   activeLoop(State).

activeLoop(State = #state{
            core = Core,
            conf = Conf = #conf{version = Vn},
            prev = Prev,
            next = Next,
            unstable = Unstable,
            stable_count = StableCount,
            next_cmd_num = NextCmdNum
   }) ->
   receive
      % Transition: add a command to stable history
      {Ref, Client, command, {Vn, Command}} when Prev == chain_head ->
         ets:insert(Unstable, {NextCmdNum, Ref, Client, Command}),
         Next ! {Ref, Client, Vn, command, NextCmdNum, Command},
         activeLoop(State#state{next_cmd_num = NextCmdNum + 1});

      % XXX: we either trust clients to send commands to the head of the chain,
      % or we have to use some sort of message authentication mechanisms
      {Ref, Client, Vn, command, NextCmdNum, Command} = Msg when Next /= chain_tail ->
         Next ! Msg,    % Forward first as this shouldn't break semantics.
         ets:insert(Unstable, {NextCmdNum, Ref, Client, Command}),
         activeLoop(State#state{next_cmd_num = NextCmdNum + 1});

      {Ref, Client, Vn, command, NextCmdNum, Command} ->    % Next == chain_tail
         Client ! {Ref, Core:do(Command)},
         Prev ! {Vn, stabilized, NextCmdNum},
         activeLoop(State#state{stable_count=StableCount+1, next_cmd_num=NextCmdNum+1});

      {Vn, stabilized, StableCount} = Msg when Prev /= chain_head ->
         Command = ets:lookup_element(Unstable, StableCount, 4),
         Core:do(Command),    % XXX: should I apply first or forward first?
         Prev ! Msg,
         ets:delete(Unstable, StableCount),
         activeLoop(State#state{stable_count = StableCount + 1});

      {Vn, stabilized, StableCount} ->    % Prev == chain_head
         Command = ets:lookup_element(Unstable, StableCount, 4),
         Core:do(Command),
         ets:delete(Unstable, StableCount),
         activeLoop(State#state{stable_count = StableCount + 1});


      % Transition: wedgeState
      % take replica into immutable state
      {Ref, Client, wedge, Vn} ->
         Client ! {Ref, wedged},
         elastic:immutableLoop(Core, Conf, Unstable, StableCount, NextCmdNum);

      % create a forked copy of this replica on this node
      {Ref, Client, fork, {Vn, ForkNode, ForkArgs}} ->
         Client ! { Ref,
            elastic:fork(Core, Conf, Unstable, StableCount, NextCmdNum,
               ForkNode, ForkArgs) },
         activeLoop(State);


      % Return current configuration
      {Ref, Client, get_conf} ->
         Client ! {Ref, Conf},
         activeLoop(State);

      % Stop this replica
      {Ref, Client, stop, Reason} ->
         Client ! {Ref, Core:stop(Reason)};


      % ignore other tagged messages and respond with current configuration
      {Ref, Client, _, _} ->
         Client ! {Ref, {error, Conf}},
         activeLoop(State);

      % ignore everything else
      _ ->
         activeLoop(State)
   end.
