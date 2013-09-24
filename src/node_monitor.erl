-module(node_monitor).
-export([
      start/1,
      op_done/1,
      register_sm/2
   ]).


-include("libdist.hrl").

-define(MONITOR, libdist_node_monitor).
-define(REPORT_INTERVAL, 1000).     % 1 second report interval


start(ClusterManager) ->
   Pid = spawn(fun() -> loop(ClusterManager) end),
   try register(?MONITOR, Pid) of
      true -> ok
   catch
      error:badarg ->   % already registered
         exit(Pid, already_started),
         already_started
   end.

op_done(LDSMServer) ->
   ?MONITOR ! {op_done, LDSMServer}.

register_sm(LDSMServer, Conf) ->
   libdist_utils:call(?MONITOR, {register_sm, LDSMServer, Conf}, infinity).


%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%

loop(ClusterManager) ->
   OpCounters = ets:new(op_counts, []),
   timer:send_interval(?REPORT_INTERVAL, report),
   loop(ClusterManager, OpCounters).

loop(ClusterManager, OpCounters) ->
   receive
      {op_done, StateMachine} ->
         ets:update_counter(OpCounters, StateMachine, 1),
         loop(ClusterManager, OpCounters);

      {Ref, Client, {register_sm, SMServer, Conf}} ->
         ets:insert(OpCounters, {SMServer, 0, Conf}),
         libdist_utils:send(ClusterManager, {register_conf, Conf}),
         Client ! {Ref, ok},
         loop(ClusterManager, OpCounters);

      report ->
         TPByConf = lists:foldl(
            fun({Server, Count, Conf}, TPAcc) ->
                  % reset operations counter in the ETS
                  true = ets:update_element(OpCounters, Server, {2, 0}),
                  % aggregate throughput by configuration
                  ServerTP = trunc(1000 * Count / ?REPORT_INTERVAL),   % Ops/Sec
                  case lists:keytake(Conf, 1, TPAcc) of
                     false ->
                        [{Conf, ServerTP} | TPAcc];
                     {value, {Conf, CurrTP}, TPWOConf} ->
                        [{Conf, ServerTP + CurrTP} | TPWOConf]
                  end
            end,
            [],
            ets:tab2list(OpCounters)
         ),
         libdist_utils:send(ClusterManager, {tp_report, TPByConf}),
         loop(ClusterManager, OpCounters);

      % ignore everything else
      _ ->
         loop(ClusterManager, OpCounters)
   end.
