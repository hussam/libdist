% ECS: Evaluate, Check, Send. Evaluates the message expression (EXP) then if the
% condition (COND) is true, the message is sent to the destination (DST)
-define(ECS(EXP, COND, DST),
   (__ECS_MSG__ = EXP), (case(COND) of true -> DST ! __ECS_MSG__; false -> do_nothing end)).

% Shorthand for libdist_utils:send(Dst, Msg)
-define(SEND(DST, MSG), libdist_utils:send(DST, MSG)).

-define(REPLY(CLT, EXP, ASE, CORE),
   (__REPLY_MSG__ = EXP), (case (ASE andalso not ldsm:is_rp_protocol(CORE)) of true -> CLT !  __REPLY_MSG__; false -> do_nothing end)).
