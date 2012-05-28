{application, repobj,
   [
      {description, "Replicated Objects"},
      {vsn, "0.1"},
      {modules, [
         repobj,
         repobj_utils,
         core,
         singleton,
         chain,
         primary_backup,
         quorum
         ]},
      {registered, []},
      {applications, [kernel, stdlib]}
   ]
}.
