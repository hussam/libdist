{application, libdist,
   [
      {description, "A library to build distributed systems"},
      {vsn, "0.2"},
      {modules, [
         libdist,
         ldsm,
         server,
         repobj,
         libdist_utils,
         singleton,
         replica,
         chain,
         primary_backup,
         quorum,
         shard_agent,
         shard,
         cache,
         elastic,
         elastic_chain,
         elastic_primary_backup,
         elastic_band,
         conf_tracker,
         libdist_client,
         node_monitor,
         cluster_manager,
         libdist_app
         ]},
      {registered, []},
      {applications, [kernel, stdlib]}
   ]
}.
