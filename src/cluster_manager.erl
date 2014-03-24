-module(cluster_manager).

-export([
      start/0,
      get_nodes/0,
      add_nodes/1,
      set_slo/1,
      deploy/3
   ]).


-include("libdist.hrl").
-include("constants.hrl").

-define(CLUSTER_MAN, libdist_cluster_manager).
-define(REEVAL_INTERVAL, 1000). % re-evaluate architecture/p-tree every 10 secs

-record(state, {
      nodes = ordsets:new(),
      slo_tree,
      flattened_slo_tree = [],
      confs_tp
   }).

-record(node, {
      conf,
      slo,
      children = []
   }).


start() ->
   Pid = spawn(fun() -> loop() end),
   try register(?CLUSTER_MAN, Pid) of
      true ->
         % a local node monitor is needed for temp replicas started while
         % building a spec.
         node_monitor:start_noreporting()
   catch
      error:badarg ->   % already registered
         exit(Pid, already_started),
         already_started
   end.

add_nodes(Nodes) ->
   libdist_utils:call(?CLUSTER_MAN, {add_nodes, Nodes}, infinity).

get_nodes() ->
   libdist_utils:call(?CLUSTER_MAN, get_nodes, infinity).

set_slo(SLO) ->
   libdist_utils:call(?CLUSTER_MAN, {set_slo, SLO}, infinity).

deploy(Module, Args, SLO) ->
   libdist_utils:call(?CLUSTER_MAN, {deploy, Module, Args, SLO}, infinity).

%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%

loop() ->
   timer:send_interval(?REEVAL_INTERVAL, reevaluate),
   State = #state{
      confs_tp = ets:new(confs_tp, []),
      slo_tree = #node{}
   },
   loop(State).

loop(State = #state{
      nodes    = Nodes,
      slo_tree = SLOTree = #node{conf = TopLevelConf},
      flattened_slo_tree = FlatSLOTree,
      confs_tp = ConfsTP
   }) ->
   receive
      {tp_report, TPReport} ->
         [ ets:update_counter(ConfsTP, Conf, TP) || {Conf, TP} <- TPReport ],
         loop(State);

      reevaluate ->
         lists:foreach(
            fun({Conf, #slo{throughput = ReqTP}}) ->
                  case ets:lookup(ConfsTP, Conf) of
                     [{_, ActualTP}] when ActualTP < ReqTP ->
                        if ActualTP /= 0 ->
                           io:format("ActualTP = ~p ReqTP = ~p\n", [ActualTP,
                                 ReqTP]);
                           true -> do_nothing
                        end,
                        % TODO: implement some action
                        todo;    % XXX: LEFT HERE!
                     _ ->
                        do_nothing
                  end,
                  % reset TP counts
                  ets:insert(ConfsTP, {Conf, 0})
            end,
            FlatSLOTree
         ),
         loop(State);

      {register_conf, NewConf} ->
         ets:insert(ConfsTP, {NewConf, 0}),
         loop(State);

      {Ref, Client, get_nodes} ->
         Client ! {Ref, Nodes},
         loop(State);

      {Ref, Client, get_conf} ->
         Client ! {Ref, TopLevelConf},
         loop(State);

      {Ref, Client, get_slo} ->
         Client ! {Ref, SLOTree#node.slo},
         loop(State);

      {Ref, Client, {add_nodes, NewNodes}} ->
         ActualNewNodes = ordsets:subtract(ordsets:from_list(NewNodes), Nodes),
         [ spawn(N, node_monitor, start, [{?CLUSTER_MAN, node()}]) || N <-
            ActualNewNodes ],
         Client ! {Ref, ok},
         loop(State#state{nodes = ordsets:union(Nodes, ActualNewNodes)});

      {Ref, Client, {set_slo, NewSLO}} ->
         NewSLOTree = build_slo_tree(TopLevelConf, NewSLO),
         NewFlattened = flatten(NewSLOTree),
         Client ! {Ref, ok},
         loop(State#state{
               slo_tree = NewSLOTree,
               flattened_slo_tree = NewFlattened
            });

      {Ref, Client, {deploy, Module, Args, NewSLO}} ->
         ConfTree = libdist:build({rptree, Module, Args, hd(Nodes)}),
         NewSLOTree = build_slo_tree(ConfTree, NewSLO),
         NewFlattened = flatten(NewSLOTree),
         NewState = State#state{
            slo_tree = NewSLOTree,
            flattened_slo_tree = NewFlattened
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


build_slo_tree(Conf, SLO) ->
   ConfMembers = case Conf#conf.type of
      ?SINGLE -> [];
      ?REPL -> Conf#conf.replicas;
      ?PART -> [ S || {_Tag, S} <- Conf#conf.partitions ]
   end,
   % XXX TODO: divide SLO changes down the tree
   Children = lists:map( fun
         (C = #conf{}) ->
            #node{conf = C, slo = SLO, children = build_slo_tree(C, SLO)};
         (P) when is_pid(P) ->
            #node{conf = P, slo = SLO, children = []}
      end,
      ConfMembers
   ),
   #node{conf = Conf, slo = SLO, children = Children}.


flatten(#node{conf = Conf, slo = SLO, children = Children}) ->
   lists:flatten([ {Conf, SLO} | [flatten(Child) || Child <- Children] ]).

