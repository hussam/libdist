% Shorthand for conditional libdist_utils:send(Dst, RId, Msg)
-define(SEND(DST, RId, MSG, COND),
   (case COND of true -> libdist_utils:send(DST, RId, MSG); false -> do_nothing end)).
