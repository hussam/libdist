{application, repobj,
   [
      {description, "Replicated Objects"},
      {vsn, "0.1"},
      {modules, [
         repobj,
         repobj_utils,
         rconf,
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
