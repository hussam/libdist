-module(elastic_chain).
-export([
      update_state/2,
      handle_msg/6
   ]).

-include("helper_macros.hrl").
-include("libdist.hrl").
-include("elastic.hrl").

update_state(Me, _Conf = #conf{replicas = NewReps}) ->
   {_, NewPrev, NewNext} = libdist_utils:ipn(Me, NewReps),
   {NewPrev, NewNext}.


handle_msg(_Me, Vn, Message, ASE = _AllowSideEffects, SM,
   State = #elastic_state{
      unstable = Unstable,
      stable_count = StableCount,
      next_cmd_num = NextCmdNum,
      pstate = {Prev, Next}
   }) ->
   case Message of
      {Ref, Client, {Vn, {read, Command}}} ->
         ?SEND(Next, {Vn, {read, Ref, Client, Command}}, ASE),
         consume;

      {Ref, Client, {Vn, {write, Command}}} ->
         ets:insert(Unstable, {NextCmdNum, Ref, Client, Command}),
         ?SEND(Next, {Vn, {write, NextCmdNum, Ref, Client, Command}}, ASE),
         {consume, State#elastic_state{next_cmd_num = NextCmdNum + 1}};

      {Vn, ProtocolMessage} ->
         case ProtocolMessage of
            {read, _, _, _} when Next /= chain_tail ->
               ?SEND(Next, Message, ASE),
               consume;

            {read, Ref, Client, Command} ->  % Next == chain_tail
               ldsm:do(SM, Ref, Client, Command, ASE),
               consume;

            {write, NextCmdNum, Ref, Client, Command} when Next /= chain_tail ->
               ?SEND(Next, Message, ASE),
               ets:insert(Unstable, {NextCmdNum, Ref, Client, Command}),
               {consume, State#elastic_state{next_cmd_num = NextCmdNum + 1}};

            {write, NextCmdNum, Ref, Client, Command} ->    % Next == chain_tail
               ldsm:do(SM, Ref, Client, Command, ASE),
               ?SEND(Prev, {Vn, {stabilized, NextCmdNum}}, ASE),
               C = NextCmdNum + 1,
               {consume, State#elastic_state{next_cmd_num=C, stable_count=C}};

            {stabilized, StableCount} ->
               Command = ets:lookup_element(Unstable, StableCount, 4),
               ldsm:do(SM, Command, false),
               if
                  Prev /= chain_head -> ?SEND(Prev, Message, ASE);
                  true -> do_not_forward
               end,
               ets:delete(Unstable, StableCount),
               {consume, State#elastic_state{stable_count = StableCount + 1}}
         end;

      % ignore everything else
      _ ->
         no_match
   end.
