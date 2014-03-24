-module(libdist_app).
-export([
      start/2,
      deploy/3
   ]).

-include("libdist.hrl").


start(ManagerNode, ClusterNodes) ->
   spawn(ManagerNode, cluster_manager, start, []),
   cluster_manager:add_nodes(ClusterNodes).

deploy(Module, Args, Throughput) ->
   SLA = #slo{ throughput = Throughput },
   cluster_manager:deploy(Module, Args, SLA).
