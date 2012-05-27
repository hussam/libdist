-record(conf, {
      version = 0,   % configuration version number
      pids = [],     % list of replicas in the configuration
      protocol,      % replication protocol to use
      args           % optional arguments to be used at protocol's discretion
   }).
