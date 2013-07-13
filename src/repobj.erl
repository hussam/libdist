-module(repobj).

% Interface for manipulating a replicated object
-export([
      new/5,
      inherit/5,
      cast/2,
      call/3,
      reconfigure/3
   ]).

-include("constants.hrl").
-include("libdist.hrl").


% Create a new replicated state machine
new(PrtclMod, PrtclArgs, SMSettings={SMModule, _} , Nodes, Timeout) ->
   % spawn new replicas
   Replicas = [ replica:new(PrtclMod, PrtclArgs, SMSettings, N) || N <- Nodes ],
   % configure replicas and return configuration
   configure(PrtclMod, PrtclArgs, SMModule, Replicas, ?NoSA, Timeout).


% Create a new replicated state machine and prepare it to later inherit the
% state of an existing process
inherit(OldPid, PrtclMod, PrtclArgs, Nodes, Timeout) ->
   % spawn new processes
   Members = case PModule:type() of
      ?REPL -> [ replica:new(PrtclMod, PrtclArgs, no_sm, N) || N <- Nodes ];
      ?PART -> [ {T, replica:new(PrtclMod, PrtclArgs, no_sm, N)} || {T,N} <- Nodes ]
   end,
   % get the old process's state machine module
   {SMModule, OldConfType} = libdist_utils:call(OldPid,
      get_sm_module_and_conf_type, Timeout),
   % create a configuration and inform all the replicas of it and return it
   ShardAgent = case (PModule:type() == ?PART) and (OldConfType /= ?SINGLE) of
      true -> OldPid;
      false -> ?NoSA
   end,
   Conf = configure(PrtclMod, PrtclArgs, SMModule, Members, ShardAgent, Timeout),
   % let the new processes inherit the appropriate portions of the state machine
   % of the old one. This also results in replacing OldPid in each process's
   % local RP-Tree with Conf
   % TODO: handle timeouts properly
   case PModule:type() of
      ?REPL ->
         libdist_utils:multicall(Members,
            {inherit_sm, OldPid, replicate, Timeout}, Timeout);
      ?PART ->
         [ libdist_utils:call(P, {inherit_sm, OldPid, {partition, Tag},
                  Timeout}, Timeout) || {Tag, P} <- Members ]
   end,
   Conf.


% Send an asynchronous command to a replicated object
cast(Conf=#conf{protocol = P}, Command) ->
   P:cast(Conf, Command).


% Send a synchronous command to a replicated object
call(Conf = #conf{protocol = P}, Command, Timeout) ->
   libdist_utils:collect(P:cast(Conf, Command), Timeout).


% Reconfigure the replicated object
reconfigure(OldConf=#conf{type=T}, NewConf=#conf{type=T}, Timeout) ->
   #conf{replicas=OldReps, partitions=OldParts, version=Vn} = OldConf,
   #conf{replicas=NewReps, partitions=NewParts} = NewConf,
   NextConf = NewConf#conf{version = Vn + 1},
   {OldPids, NewPids} = case T of
      ?SINGLE -> {OldReps, NewReps};
      ?REPL -> {OldReps, NewReps};
      ?PART -> { [P || {_,P} <- OldParts], [P || {_,P} <- NewParts]}
   end,
   % This takes out the members of the old configuration but not in the new one
   Refs = libdist_utils:multicast(OldPids, {reconfigure, NextConf}),
   case libdist_utils:collectall(Refs, Timeout) of
      {ok, _} ->
         % This integrates members of the new configuration that are not old
         Refs2 = libdist_utils:multicast(NewPids, {reconfigure, NextConf}),
         case libdist_utils:collectall(Refs2, Timeout) of
            {ok, _} ->
               {ok, NextConf};
            Error ->
               Error
         end;
      % TODO: distinguish which case of timeout occurred
      Error ->
         Error
   end.



%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%

% Configure a set of processes for the first time
configure(PrtclModule, PrtclArgs, SMModule, Members, ShardAgent, Timeout) ->
   Conf = make_conf(PrtclModule, PrtclArgs, SMModule, Members, ShardAgent),
   Conf0 = Conf#conf{replicas = [], partitions = []},
   reconfigure(Conf0, Conf, Timeout).


% Create a distributed system configuration
make_conf(Protocol, ProtocolArgs, SMModule, Members, ShardAgent) ->
   case Protocol:type() of
      ?SINGLE ->
         #conf{
            type = ?SINGLE,
            protocol = Protocol,
            sm_mod = SMModule,
            replicas = Members
         };
      ?REPL ->
         #conf{
            type = ?REPL,
            protocol = Protocol,
            sm_mod = SMModule,
            replicas = Members,
            args = Protocol:conf_args(ProtocolArgs)
         };
      ?PART ->
         {RouteFn, PArgs} = ProtocolArgs,
         #conf{
            type = ?PART,
            protocol = Protocol,
            sm_mod = SMModule,
            partitions = Members,
            shard_agent = ShardAgent,
            route_fn = RouteFn,
            args = Protocol:conf_args(PArgs)
         }
   end.
