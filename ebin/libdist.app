{application, libdist,
   [
      {description, "A library to build distributed systems"},
      {vsn, "0.2"},
      {modules, [
         libdist_sm,
         server,
         repobj,
         libdist_utils,
         singleton,
         chain,
         primary_backup,
         quorum,
         elastic,
         elastic_chain,
         elastic_primary_backup
         ]},
      {registered, []},
      {applications, [kernel, stdlib]}
   ]
}.
