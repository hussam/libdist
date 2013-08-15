-module(libdist_client).

-compile({inline, [get_req_params/1]}).

-export([
      connect/2,
      cast/2,
      call/2,
      get_conf/1
   ]).


-include("libdist.hrl").

-define(PROXY, libdist_tracker_proxy).

connect(TrackerNode, Timeout) ->
   case conf_tracker:ping(TrackerNode, Timeout) of
      {ok, Tracker} ->
         Ref = make_ref(),
         Self = self(),
         spawn(fun() -> start_tracker_proxy(Tracker, Timeout, Ref, Self) end),
         receive
            {Ref, ClientId} -> {ok, ClientId}
         end;

      {error, Reason} ->
         {error, {couldnt_connect_to_tracker, Reason}}
   end.

% Send an asynchronous command to a distributed service
cast(ClientId, Command) ->
   case get_req_params(ClientId) of
      {ok, Conf=#conf{protocol=P}, _Timeout} -> P:cast(Conf, Command);
      Error -> Error
   end.

% Send a synchronous command to a distributed service
call(ClientId, Command) ->
   case get_req_params(ClientId) of
      {ok, Conf=#conf{protocol=P}, Timeout} ->
         libdist_utils:collect(P:cast(Conf, Command), Timeout);
      Error ->
         Error
   end.


% Get the configuration that will be used by the given client Id
get_conf(ClientId) ->
   case get_req_params(ClientId) of
      {ok, Conf, _} -> Conf;
      Error -> Error
   end.


%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%


start_tracker_proxy(Tracker, Timeout, Ref, Client) ->
   try register(?PROXY, self()) of
      true ->
         {ok, Conf} = conf_tracker:client_connect(Tracker, Timeout),
         ?PROXY ! {Ref, Client, reg_client, Timeout},
         proxy_loop(Tracker, Conf, dict:new())
   catch
      error:badarg ->   % already_registered
         ?PROXY ! {Ref, Client, reg_client, Timeout}
   end.


proxy_loop(Tracker, Conf=#conf{version = Vn}, Clients) ->
   receive
      {Ref, Client, reg_client, Timeout} ->
         ClientId = dict:size(Clients) + 1,
         Client ! {Ref, ClientId},
         proxy_loop(Tracker, Conf, dict:store(ClientId, Timeout, Clients));

      {Ref, Client, get_req_params, ClientId} ->
         case dict:find(ClientId, Clients) of
            {ok, ClientTimeout} -> Client ! {Ref, {ok, Conf, ClientTimeout}};
            error -> Client ! {Ref, {error, client_not_found}}
         end,
         proxy_loop(Tracker, Conf, Clients);

      {tracker_updated_conf, NewConf=#conf{version = Vn2}} when Vn2 > Vn ->
         proxy_loop(Tracker, NewConf, Clients);

      {Client, get_clients} ->
         Client ! {clients, Clients},
         proxy_loop(Tracker, Conf, Clients);

      _ ->     % ignore everything else
         proxy_loop(Tracker, Conf, Clients)
   end.


get_req_params(ClientId) ->
   Ref = make_ref(),
   ?PROXY ! {Ref, self(), get_req_params, ClientId},
   receive {Ref, Response} -> Response end.


