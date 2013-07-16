% Default timeout used internally by Elastic Bands/Replication
-define(E_TO, 1000).    % 1 second

% State specific to an elastic replica
-record(elastic_state, {
      conf_version = 0,
      mode         = pending,
      stable_count = 0,
      next_cmd_num = 0,
      unstable,
      protocol,
      pstate,
      timeout
   }).
