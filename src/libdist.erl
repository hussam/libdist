-module(libdist).
-include("magic_numbers.hrl").

-export([
      new_proc/3,
      replicate/4
   ]).


-include("libdist.hrl").

% Create a new standalone server process
new_proc(Node, Module, Args) ->
   singleton:new(Node, Module, Args).


replicate(Pid, Protocol, Args, Nodes) when is_pid(Pid) ->
   % create a replicated state machine in place of the Pid
   {ok, PidConf} = Protocol:new_from(Pid, Args, Nodes, ?TO),

   % the processes in PidConf will have core state machines for all R/P nodes
   % from Pid to the root of the RP Tree. These state machines will also have
   % references to the different configurations along the way. So, an
   % 'integrate/replace' command can be issued to PidConf and the required
   % reconfiguration commands will percolate all the way to the top of the RP
   % Tree.
   NewRootConf = libdist_utils:call(Pid, {replace, Pid, PidConf}, ?TO),

   %Protocol:call(Pid, {stop, replaced}),
   NewRootConf.


%%%%%%%%%%%%%%%%%%%%%
% Utility Functions %
%%%%%%%%%%%%%%%%%%%%%


% Replace Pid in Conf with NewItem
replace_pid(Conf = #rconf{pids = Pids}, Pid, NewItem) ->
   case replace_pid([], Pids, Pid, NewItem) of
      false ->
         Conf;

      NewPids ->
         Conf#rconf{pids = NewPids}
   end.


replace_pid(_, [], _, _) ->
   false;

replace_pid(Preds, [Pid | Tail], Pid, NewItem) ->
   lists:reverse(Preds) ++ [NewItem | Tail];

replace_pid(Preds, [Conf = #rconf{pids = Pids} | Tail], Pid, NewItem) ->
   case replace_pid([], Pids, Pid, NewItem) of
      false ->
         replace_pid([Conf | Preds], Tail, Pid, NewItem);
      NewPids ->
         lists:reverse(Preds) ++ [Conf#rconf{pids = NewPids} | Tail]
   end;

replace_pid(Preds, [P | Tail], Pid, NewItem) ->
   replace_pid([P | Preds], Tail, Pid, NewItem).

