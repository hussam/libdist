-module(rconf).

-export([
      call/3
   ]).

% Interface for examining and manipulating a configuration
-export([
      version/1,  set_version/2,
      pids/1,     set_pids/2,
      protocol/1, set_protocol/2,
      args/1,     set_args/2
   ]).

-include("libdist.hrl").



call(Conf = #rconf{protocol = P}, {do, Command}, Retry) ->
   P:do(Conf, Command, Retry);

call(Conf = #rconf{protocol = P}, {reconfigure, NewReplicas, NewArgs}, Retry) ->
   P:reconfigure(Conf, NewReplicas, NewArgs, Retry);

call(Conf = #rconf{protocol = P}, {stop, N, Reason}, Retry) ->
   P:stop(Conf, N, Reason, Retry).




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Interface for examining and manipulating a repobj configuration %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

version(#rconf{version = Vn}) -> Vn;
version(_) -> not_a_repobj.

set_version(Conf = #rconf{}, NewVn) -> Conf#rconf{version = NewVn};
set_version(_, _) -> not_a_repobj.

pids(#rconf{pids = Pids}) -> Pids;
pids(_) -> not_a_repobj.

set_pids(Conf = #rconf{}, NewPids) -> Conf#rconf{pids = NewPids};
set_pids(_, _) -> not_a_repobj.

protocol(#rconf{protocol = Protocol}) -> Protocol;
protocol(_) -> not_a_repobj.

set_protocol(Conf = #rconf{}, NewProtocol) -> Conf#rconf{protocol = NewProtocol};
set_protocol(_, _) -> not_a_repobj.

args(#rconf{args = Args}) -> Args;
args(_) -> not_a_repobj.

set_args(Conf = #rconf{}, NewArgs) -> Conf#rconf{args = NewArgs};
set_args(_, _) -> not_a_repobj.



