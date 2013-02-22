% ECS: Evaluate, Check, Send. Evaluates the message expression (EXP) then if the
% condition (COND) is true, the message is sent to the destination (DST)
-define(ECS(EXP, COND, DST),
   (__ECS_MSG__ = EXP), (case(COND) of true -> DST ! __ECS_MSG__; false -> do_nothing end)).
