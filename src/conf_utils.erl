% Set of utility functions for manipulating configuration (#conf{}) records.
-module(conf_utils).
-export([
      member/2,
      peers/2
   ]).

-include("constants.hrl").
-include("libdist.hrl").


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


