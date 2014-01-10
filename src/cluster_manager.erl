-module(cluster_manager).

-compile({inline, [store/3]}).

-export([
      start/0,
      get_nodes/0,
      add_nodes/1,
      set_sla/1,
      deploy/3
   ]).


-include("libdist.hrl").

-define(CLUSTER_MAN, libdist_cluster_manager).
-define(REEVAL_INTERVAL, 10000). % re-evaluate architecture/p-tree every 10 secs

-record(state, {
      nodes = ordsets:new(),
      conf_tree,
      reqs_tree = [],
      confs_tp,
      sla
   }).


start() ->
   Pid = spawn(fun() -> loop() end),
   try register(?CLUSTER_MAN, Pid) of
      true -> ok
   catch
      error:badarg ->   % already registered
         exit(Pid, already_started),
         already_started
   end.

add_nodes(Nodes) ->
   libdist_utils:call(?CLUSTER_MAN, {add_nodes, Nodes}, infinity).

get_nodes() ->
   libdist_utils:call(?CLUSTER_MAN, get_nodes, infinity).

set_sla(SLA) ->
   libdist_utils:call(?CLUSTER_MAN, {set_sla, SLA}, infinity).

deploy(Module, Args, SLA) ->
   libdist_utils:call(?CLUSTER_MAN, {deploy, Module, Args, SLA}, infinity).

%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%

loop() ->
   timer:send_interval(?REEVAL_INTERVAL, reevaluate),
   State = #state{
      confs_tp = ets:new(confs_tp, [])
   },
   loop(State).

loop(State = #state{
      nodes     = Nodes,
      conf_tree = _ConfTree,
      reqs_tree = ReqsTree,
      confs_tp  = ConfsTP,
      sla       = _SLA
   }) ->
   receive
      {tp_report, TPReport} ->
         [ ets:update_counter(ConfsTP, Conf, TP) || {Conf, TP} <- TPReport ],
         loop(State);

      reevaluate ->
         lists:foreach(
            fun({Conf, #sla{throughput = ReqTP}}) ->
                  case ets:lookup(ConfsTP, Conf) of
                     [{_, ActualTP}] when ActualTP < ReqTP ->
                        % TODO: implement some action
                        todo;    % XXX: LEFT HERE!
                     _ ->
                        do_nothing
                  end,
                  % reset TP counts
                  ets:insert(ConfsTP, {Conf, 0})
            end,
            ReqsTree
         ),
         loop(State);

      {register_conf, NewConf} ->
         ets:insert(ConfsTP, {NewConf, 0}),
         loop(State);

      {Ref, Client, get_nodes} ->
         Client ! {Ref, Nodes},
         loop(State);

      {Ref, Client, {add_nodes, NewNodes}} ->
         Client ! {Ref, ok},
         ActualNewNodes = ordsets:subtract(ordsets:from_list(NewNodes), Nodes),
         [ spawn(N, node_monitor, start, [{?CLUSTER_MAN, node()}]) || N <-
            ActualNewNodes ],
         loop(State#state{nodes = ordsets:union(Nodes, ActualNewNodes)});

      {Ref, Client, {set_sla, NewSLA}} ->
         Client ! {Ref, ok},
         loop(State#state{sla = NewSLA});

      {Ref, Client, {deploy, Module, Args, NewSLA}} ->
         NewConfTree = libdist:build({rptree, Module, Args, hd(Nodes)}),
         NewState = State#state{
            conf_tree = NewConfTree,
            reqs_tree = store(NewConfTree, NewSLA, ReqsTree),
            sla = NewSLA
         },
         Client ! {Ref, ok},
         loop(NewState);

      % ignore everything else
      _ ->
         loop(State)
   end.


%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%


store(Key, Value, KeyList) ->
   lists:keystore(Key, 1, KeyList, {Key, Value}).
