-module(repobj).

% Interface for manipulating a replicated object
-export([
      new/4,
      inherit/4,
      cast/2,
      cast/3,
      call/3,
      call/4,
      reconfigure/3
   ]).

-include("constants.hrl").
-include("libdist.hrl").


% Create a new replicated state machine
new(PSettings={PModule, _}, SMSettings={SMModule, _} , Nodes, Timeout) ->
   % spawn new replicas
   Replicas = [ replica:new(PModule, SMSettings, N) || N <- Nodes ],
   % configure replicas and return configuration
   configure(PSettings, SMModule, Replicas, Timeout).


% Create a new replicated state machine and prepare it to later inherit the
% state of an existing process
inherit(Pid, PSettings = {PModule, _}, Nodes, Timeout) ->
   % spawn new processes
   Members = case PModule:type() of
      ?REPL -> [ replica:new(PModule, no_sm, N) || N <- Nodes ];
      ?PART -> [ {T, replica:new(PModule, no_sm, N)} || {T,N} <- Nodes ]
   end,
   % get the process's state machine module
   SMModule = libdist_utils:call(Pid, get_sm_module, Timeout),
   % create a configuration and inform all the replicas of it and return it
   configure(PSettings, SMModule, Members, Timeout).


% Send an asynchronous command to a replicated object
cast(Conf, Command) ->
   cast(Conf, ?ALL, Command).

cast(Conf=#conf{protocol = P}, CRId, Command) ->
   P:cast(Conf, CRId, Command).


% Send a synchronous command to a replicated object
call(Conf, Command, Timeout) ->
   call(Conf, ?ALL, Command, Timeout).

call(Conf = #conf{protocol = P}, CRId, Command, Timeout) ->
   libdist_utils:collect(P:cast(Conf, CRId, Command), Timeout).


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
configure(ProtocolSettings, SMModule, Members, Timeout) ->
   Conf = make_conf(ProtocolSettings, SMModule, Members),
   Conf0 = Conf#conf{replicas = [], partitions = []},
   reconfigure(Conf0, Conf, Timeout).


% Create a distributed system configuration
make_conf({Protocol, ProtocolArgs}, SMModule, Members) ->
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
            route_fn = RouteFn,
            args = Protocol:conf_args(PArgs)
         }
   end.
