-module(libdist).

-export([
      new_proc/3,
      replicate/4
   ]).


-include("constants.hrl").
-include("libdist.hrl").

% TODO: FIXME: Remove this magic number
-define(TO, 1000).

% Create a new standalone server process
new_proc(Node, SMModule, SMArgs) ->
   replica:new(singleton, {SMModule, SMArgs}, Node).


replicate(Pid, Protocol, Args, Nodes) when is_pid(Pid) ->
   % create a replicated state machine in place of the Pid
   {ok, Conf} = repobj:inherit(Pid, {Protocol, Args}, Nodes, ?TO),
   replace(Pid, Conf, ?TO).



%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%

replace(Pid, Conf=#conf{type=CType, replicas=Reps, partitions=Parts}, Timeout) ->
   % the process Pid has nested state machines for all R/P nodes from Pid to the
   % root of the RP Tree. These state machines will also have references to the
   % different configurations along the way. So, an 'replace' command can be
   % issued to Pid and the required reconfiguration commands will percolate all
   % the way to the top of the RP Tree.

   NewPids = case CType of
      ?REPL -> Reps;
      ?PART -> [ P || {_, P} <- Parts ]
   end,

   % replace the old process with the new conf and get the new root conf
   NewRootConf = libdist_utils:call(Pid, {replace, Pid, Conf}, Timeout),
   % let the new processes inherit the state machine of the old one
   % TODO: implement some sort of background state transfer
   libdist_utils:multicall(NewPids, {inherit_sm, Pid, Timeout}, Timeout),
   libdist_utils:call(Pid, {stop, replaced}, Timeout),
   NewRootConf.

