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
         elastic,
         elastic_chain,
         elastic_primary_backup
         ]},
      {registered, []},
      {applications, [kernel, stdlib]}
   ]
}.
