-module(echo).
-behaviour(ldsm).

% Echo Public API
-export([
      start/3,
      start_chain/1,
      start_pb/1,
      echo/2,
      set_tag/2,
      stop_replica/3
   ]).

% libdist state machine behaviour interface
-export([
      init_sm/1,
      handle_cmd/3,
      is_mutating/1,
      stop/2,
      export/1,
      import/1
   ]).


% 5 second timeout
-define(TIMEOUT, 5000).

% Start a replicated 'echo' server given using RepProtocol on a bunch of Nodes
start(RepProtocol, RepArgs, Nodes) ->
   % Create a new replicated object.
   % The function 'repobj:new/3' takes 3 parameters, the first is a tuple
   % specifying the core module's name and list of arguments to pass to its
   % new/1 function. The second is a tuple specifying the replication protocol,
   % arguments to the replication protocol (if any), and a list of nodes to
   % deploy the replicated object on. The last parameter is a measure for when
   % to timeout on trying to create the object if things don't go right.
   repobj:new({?MODULE, [echo_server]}, {RepProtocol, RepArgs, Nodes}, ?TIMEOUT).

% Starts a chain replicated 'echo' server on a bunch of nodes
start_chain(Nodes) ->
   repobj:new({?MODULE, [echo_server]}, {chain, [], Nodes}, ?TIMEOUT).

% Starts a primary/backup replicated 'echo' server on a bunch of nodes
start_pb(Nodes) ->
   repobj:new({?MODULE, [echo_server]}, {primary_backup, [], Nodes}, ?TIMEOUT).

% Send a message to be echoed by the replicated object
echo(Obj, Message) ->
   repobj:call(Obj, {echo, Message}, ?TIMEOUT).

% Updated the tag used by the echo server
set_tag(Obj, NewTag) ->
   repobj:call(Obj, {set_tag, NewTag}, ?TIMEOUT).

% Stop the nth replica of a replicated object
stop_replica(Obj, N, Reason) ->
   repobj:stop(Obj, N, Reason, ?TIMEOUT).



% All the functions below are just implementing the state machine interface


% The new/1 function takes in a bunch of arguments and returns the id of a new
% local instance of the core. In this case, the arguments contain a tag used by
% the echo server.
init_sm(_Args = [Tag]) ->
   Tag.

% The handle_cmd/3 function takes in a local state, a command, and a flag of
% whether side effects are allowed, and executes the command. The echo server
% understands two types of commands, 'set_tag' which modifies the internal state
% of the server, and 'echo' which is a 'readonly' command that echoes the passed
% in message. The side effects flag is meaningless here because the echo server
% does not communicate with the outside world
handle_cmd(_OldTag, {set_tag, NewTag}, _) ->
   {reply, ok, NewTag};
handle_cmd(Tag, {echo, Message}, _) ->
   {reply, {Tag, Message}};
handle_cmd(_, _, _) ->
   {reply, undefined_op}.


% The is_mutating/1 function takes in a command and returns true/false if the
% command will change the internal state of the local core or not.
is_mutating({set_tag, _}) ->
   true;
is_mutating({echo, _}) ->
   false.


% The stop/2 function stops the specified instance given the reason passed to it
% by the libdist. This could be due to reconfiguration or a specific user
% request.
stop(Tag, Reason) ->
   io:format("Stopping ~p because ~p\n", [Tag, Reason]).


% The export/1 function allows a state machine to cleanly export its local state
% so that it could later be imported by another state machine. Here, exporting
% the local state is easy, just return it!
export(Tag) ->
   Tag.


% The import/1 function imports a previous exported state machine state. Here,
% the local state is just a simple tag value.
import(Tag) ->
   Tag.
