-module(libdist).

-export([
      new_proc/3,
      replicate/4,
      partition/5,
      build/1
   ]).


-include("constants.hrl").
-include("libdist.hrl").

% TODO: FIXME: Remove this magic number
-define(TO, 1000).

% Create a new standalone server process
new_proc(SMModule, SMArgs, Node) ->
   replica:new(singleton, {SMModule, SMArgs}, Node).


% the process Pid has nested state machines for all R/P nodes from Pid to the
% root of the RP Tree. These state machines will also have references to the
% different configurations along the way. So, an 'replace' command can be
% issued to Pid and the required reconfiguration commands will percolate all
% the way to the top of the RP Tree.


replicate(OldPid, Protocol, Args, Nodes) when is_pid(OldPid) ->
   {_, RootConf} = do_replicate(OldPid, Protocol, Args, Nodes),
   RootConf.

do_replicate(OldPid, Protocol, Args, Nodes) ->
   % create a replicated state machine in place of the OldPid
   {ok, Conf} = repobj:inherit( OldPid, Protocol, Args, Nodes, ?TO),
   % replace the old process with the new configuration in the RP-Tree
   {Conf, libdist_utils:call(OldPid, {replace, OldPid, Conf}, ?TO)}.


partition(OldPid, Protocol, Args, RouteFn, SplitFn) when is_pid(OldPid) ->
   {_, RootConf} = do_partition(OldPid, Protocol, Args, RouteFn, SplitFn),
   RootConf.

do_partition(OldPid, Protocol, Args, RouteFn, SplitFn) ->
   % get the tags currently associated with the process
   OldTags = libdist_utils:call(OldPid, get_tags, ?TO),
   {ok, Conf} = repobj:inherit(
         OldPid, Protocol, {RouteFn, Args}, SplitFn(OldTags), ?TO),
   % replace the old process with the new configuration in the RP-Tree
   {Conf, libdist_utils:call(OldPid, {replace, OldPid, Conf}, ?TO)}.


% Build an RP-Tree using top-level specification
build({rptree, SMModule, SMArgs, Container}) ->
   case Container of
      Node when is_atom(Node) ->
         {ok,Conf} = repobj:new(singleton, [], {SMModule, SMArgs}, [Node], ?TO),
         Conf;
      Spec ->
         TopLevelSM = replica:new(singleton, [], {SMModule, SMArgs}, node()),
         build(TopLevelSM, Spec)
   end;

% Build an RP-Tree as specified in Filename
build(Filename) ->
   case file:consult(Filename) of
      {ok, [Spec]} -> build(Spec);
      Error -> Error
   end.


%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%


build(ParentPid, {rsm, Protocol, Args, Containers}) ->
   {Conf, TmpRootConf} = do_replicate(ParentPid, Protocol, Args,
      lists:map(fun(C) when is_atom(C) -> C; (_) -> node() end, Containers)),
   lists:foldl(
      fun
         ({Node, _Pid}, RootConf) when is_atom(Node) -> RootConf;
         ({Spec, Pid}, _RootConf) -> build(Pid, Spec)
      end,
      TmpRootConf,
      lists:zip(Containers, Conf#conf.replicas)
   );

build(ParentPid, {psm, Protocol, Args, _RouteFn={Mod, Fn}, TaggedContainers}) ->
   TaggedNodes = lists:map(
      fun ({T,C}) when is_atom(C) -> {T,C}; ({T, _}) -> {T,node()} end,
      TaggedContainers
   ),
   SplitFn = fun(_) -> TaggedNodes end,
   {Conf, TmpRootConf} = do_partition(ParentPid, Protocol, Args, fun Mod:Fn/2, SplitFn),
   lists:foldl(
      fun
         ({{T, Node}, {T, _Pid}}, RootConf) when is_atom(Node) -> RootConf;
         ({{T, Spec}, {T, Pid}}, _RootConf) -> build(Pid, Spec)
      end,
      TmpRootConf,
      lists:zip(TaggedContainers, Conf#conf.partitions)
   ).
