-module(libdist).

-export([
      new_proc/3,
      replicate/4
   ]).


-include("libdist.hrl").

% TODO: FIXME: Remove this magic number
-define(TO, 1000).

% Create a new standalone server process
new_proc(Node, Module, Args) ->
   singleton:new(Node, Module, Args).


replicate(Pid, Protocol, Args, Nodes) when is_pid(Pid) ->
   % create a replicated state machine in place of the Pid
   repobj:inherit(Pid, {Protocol, Args}, Nodes, ?TO).

   % the processes in PidConf will have core state machines for all R/P nodes
   % from Pid to the root of the RP Tree. These state machines will also have
   % references to the different configurations along the way. So, an
   % 'integrate/replace' command can be issued to PidConf and the required
   % reconfiguration commands will percolate all the way to the top of the RP
   % Tree.

   %NewRootConf = libdist_utils:call(Pid, {replace, Pid, PidConf}, ?TO),

   %Protocol:call(Pid, {stop, replaced}),
   %NewRootConf.


%partition(Conf, Pid, Split, Protocol, RouteFn) when is_pid(Pid) ->
%   Tag = get_tag(
%   TagsAndNodes = SplitFn(),
%   todo.
