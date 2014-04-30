% Set of utility functions for manipulating configuration (#conf{}) records.
-module(conf_utils).
-export([
      members/1,
      member/2,
      peers/2,
      all_procs/1
   ]).

-compile({inline, [members/1]}).

-include("constants.hrl").
-include("libdist.hrl").


% Return all the members of the given configuration
members(#conf{type=ConfType, replicas=Replicas, partitions=Partitions}) ->
   case ConfType of
      ?PART -> [ M || {_Tag, M} <- Partitions ];
      _ -> Replicas
   end.


% Test whether a process or PSM is a member of a given configuration
member(Elem, #conf{type=ConfType, replicas=Replicas, partitions=Partitions}) ->
   case ConfType of
      ?PART -> lists:keymember(Elem, 2, Partitions);
      _ -> lists:member(Elem, Replicas)
   end.


% Return the peers of a process or PSM in the given configuration
peers(Me, #conf{type=ConfType, replicas=Replicas, partitions=Partitions}) ->
   case ConfType of
      ?PART -> lists:delete(Me, [ P || {_Tag, P} <- Partitions ]);
      _ ->     lists:delete(Me, Replicas)
   end.


% Return all the processes contained in the given configuration (at all levels)
all_procs(Conf) ->
   lists:flatten(
      lists:map( fun
            (M) when is_pid(M) -> M;
            (M) -> all_procs(M)
         end,
         members(Conf)
      )
   ).

