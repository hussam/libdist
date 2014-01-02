% Shorthand for conditional libdist_utils:send(Dst, Msg)
-define(SEND(DST, MSG, COND),
   (case COND of true -> libdist_utils:send(DST, MSG); false -> do_nothing end)).

-define(SEND_AFTER(TIME, DST, MSG, COND),
   (case COND of true -> libdist_utils:send_after(TIME, DST, MSG); false -> do_nothing end)).
