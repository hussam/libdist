-module(libdist).

-export([
      new_proc/3,
      replicate/4,
      partition/5
   ]).


-include("constants.hrl").
-include("libdist.hrl").

% TODO: FIXME: Remove this magic number
-define(TO, 1000).

% Create a new standalone server process
new_proc(Node, SMModule, SMArgs) ->
   replica:new(singleton, {SMModule, SMArgs}, Node).


% the process Pid has nested state machines for all R/P nodes from Pid to the
% root of the RP Tree. These state machines will also have references to the
% different configurations along the way. So, an 'replace' command can be
% issued to Pid and the required reconfiguration commands will percolate all
% the way to the top of the RP Tree.


replicate(OldPid, Protocol, Args, Nodes) when is_pid(OldPid) ->
   % create a replicated state machine in place of the OldPid
   {ok, Conf=#conf{replicas=NewPids}} = repobj:inherit(
      OldPid, {Protocol, Args}, Nodes, ?TO),
   % let the new replicas inherit the state machine of the old one. This also
   % results in replacing OldPid in each NewPid's local RP-Tree with Conf
   libdist_utils:multicall(NewPids, {inherit_sm, OldPid, all, ?TO}, ?TO),
   % replace the old process with the new configuration in the RP-Tree
   libdist_utils:call(OldPid, {replace, OldPid, Conf}, ?TO).


partition(OldPid, Protocol, Args, SplitFn, RouteFn) when is_pid(OldPid) ->
   % get the tags currently associated with the process
   OldTags = libdist_utils:call(OldPid, get_tags, ?TO),
   {ok, Conf=#conf{partitions=NewPartitions}} = repobj:inherit(
         OldPid, {Protocol, {RouteFn, Args}}, SplitFn(OldTags), ?TO),
   % let processes inherit their own of the old process's state. This also
   % results in replacing OldPid in each NewPid's local RP-Tree with Conf
   [
      libdist_utils:call(Pid, {inherit_sm, OldPid, {part, Tag}, ?TO}, ?TO) ||
      {Tag, Pid} <- NewPartitions
   ],
   % replace the old process with the new configuration in the RP-Tree
   libdist_utils:call(OldPid, {replace, OldPid, Conf}, ?TO).


%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%

