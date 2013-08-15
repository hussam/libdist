-module(conf_tracker).
-export([
      track/1,
      track/2,
      get_conf/1,
      update_conf/2,

      ping/2,
      client_connect/2
   ]).

-export([
      run_local_tracker/1
   ]).

-include("constants.hrl").
-include("libdist.hrl").

-define(TRACKER, libdist_conf_tracker).
-define(GOSSIP_INT, 5000).    % gossip interval


% Start a tracker on all the nodes of the given configuration (if needed) and
% track that configuration
track(Conf) ->
   [ track(N, Conf) || N <- ordsets:to_list(get_conf_nodes(Conf)) ],
   ok.

% Start a tracker on the given node and add the passed-in configuration
track(Node, Conf) ->
   Pid = spawn(Node, ?MODULE, run_local_tracker, [?GOSSIP_INT]),
   Tracker = case rpc:call(Node, erlang, register, [?TRACKER, Pid]) of
      true ->
         {?TRACKER, Node};
      {badrpc, _} ->    % already registered
         % TODO: make sure badrpc is because of already registered tracker
         % rather than any other reason
         exit(Pid, already_registered),
         {?TRACKER, Node}
   end,
   update_conf(Tracker, Conf),
   Tracker.


% Update the configuration at the tracker
update_conf(Tracker, NewConf) ->
   Ref = make_ref(),
   Tracker ! {Ref, self(), update_conf, NewConf},
   receive
      {Ref, ok} -> ok
   end.


% Get the configuration at the tracker
get_conf(Tracker) ->
   Ref = make_ref(),
   Tracker ! {Ref, self(), get_conf},
   receive
      {Ref, Conf} -> Conf
   end.


% Start a configuration tracker on the local node.
run_local_tracker(GossipInterval) ->
   {A1, A2, A3} = now(),
   random:seed(A1, A2, A3),
   timer:send_interval(GossipInterval, do_gossip),
   loop(#conf{version = 0}, [], [], 0).


%%%%%%%%%%%%%%%%%%%%
% Client Functions %
%%%%%%%%%%%%%%%%%%%%


% Ping the tracker on the given node
ping(TrackerNode, Timeout) ->
   Ref = make_ref(),
   Tracker = {?TRACKER, TrackerNode},
   Tracker ! {Ref, self(), ping},
   receive
      {Ref, pong} -> {ok, Tracker}
   after
      Timeout ->
         {error, timeout}
   end.

% Connects a client to a tracker
client_connect(Tracker, Timeout) ->
   Ref = make_ref(),
   Tracker ! {Ref, self(), client_connect},
   receive
      {Ref, Conf} -> {ok, Conf}
   after
      Timeout -> {error, timeout}
   end.


%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%


% Main tracker message loop
loop(Conf = #conf{version = Vn}, RegClients, PeerNodes, NumPeers) ->
   receive
      {Ref, Client, ping} ->
         Client ! {Ref, pong},
         loop(Conf, RegClients, PeerNodes, NumPeers);

      {Ref, NewClient, client_connect} ->
         NewClient ! {Ref, Conf},
         loop(Conf, [NewClient | RegClients], PeerNodes, NumPeers);

      {Ref, Client, get_conf} ->
         Client ! {Ref, Conf},
         loop(Conf, RegClients, PeerNodes, NumPeers);

      {Ref, Client, update_conf, NewConf=#conf{version = Vn2}} when Vn2 > Vn ->
         Client ! {Ref, ok},
         NewPeerNodes = lists:delete(node(),
            ordsets:to_list(get_conf_nodes(NewConf))),
         [ C ! {tracker_updated_conf, NewConf} || C <- RegClients ],
         loop(NewConf, RegClients, NewPeerNodes, length(NewPeerNodes));

      do_gossip when NumPeers > 0 ->
         GossipPeer = lists:nth(random:uniform(NumPeers), PeerNodes),
         {?TRACKER, GossipPeer} ! {gossip, self(), Conf},
         loop(Conf, RegClients, PeerNodes, NumPeers);

      {gossip, Peer, _PeerConf = #conf{version = Vn2}} when Vn2 < Vn ->
         Peer ! {gossip, self(), Conf},
         loop(Conf, RegClients, PeerNodes, NumPeers);

      {gossip, _Peer, PeerConf = #conf{version = Vn2}} when Vn2 > Vn ->
         NewPeerNodes = lists:delete(node(),
            ordsets:to_list(get_conf_nodes(PeerConf))),
         [ C ! {tracker_updated_conf, PeerConf} || C <- RegClients ],
         loop(PeerConf, RegClients, NewPeerNodes, length(NewPeerNodes));

      _ ->  % ignore everything else
         loop(Conf, RegClients, PeerNodes, NumPeers)
   end.

% Returns an ordered set of all the nodes 
get_conf_nodes(#conf{type = ?PART, partitions = Partitions}) ->
   get_nodes([PidOrConf || {_Tag, PidOrConf} <- Partitions]);
get_conf_nodes(#conf{replicas = Replicas}) ->
   get_nodes(Replicas).

get_nodes(ConfMembers) ->
   lists:foldl(
      fun
         (Pid, Nodes) when is_pid(Pid) -> ordsets:add_element(node(Pid), Nodes);
         (Conf, Nodes) -> ordsets:union(get_conf_nodes(Conf), Nodes)
      end,
      ordsets:new(),
      ConfMembers
   ).



