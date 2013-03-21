% Distributed state machine configuration
-record(conf, {
      version = 0,      % configuration version number
      type,             % rconf or pconf
      protocol,         % replication or partitioning protocol to use
      sm_mod,           % module for internal state machine
      replicas = [],    % list of replicas in the configuration
      partitions = [],  % list of partitions in the configuration
      route_fn,         % routing function (for partition configurations)
      args              % optional arguments to be used at protocol's discretion
   }).
