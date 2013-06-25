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
         cache
         ]},
      {registered, []},
      {applications, [kernel, stdlib]}
   ]
}.
