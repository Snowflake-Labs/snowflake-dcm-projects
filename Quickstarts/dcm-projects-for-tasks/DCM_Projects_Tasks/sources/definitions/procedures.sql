/*=============================================================================
  procedures.sql — SQL stored procedures used by demo tasks

  Uses the new DCM `DEFINE PROCEDURE` statement (early-access) so procedure
  lifecycle is fully managed by DCM — no separate CREATE OR ALTER.
=============================================================================*/

-- Always-succeeds procedure
DEFINE PROCEDURE DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_PROCEDURE_1()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
    SELECT SYSTEM$WAIT(3);
$$;

-- Fails ~50% of the time by selecting from a missing table
DEFINE PROCEDURE DCM_DEMO_4{{env_suffix}}.PIPELINE.DEMO_PROCEDURE_2()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
    DECLARE
        RANDOM_VALUE NUMBER(2,0);
    BEGIN
        RANDOM_VALUE := (SELECT UNIFORM(1, 2, RANDOM()));
        IF (:RANDOM_VALUE = 2) THEN
            SELECT COUNT(*) FROM OLD_TABLE;   -- intentional failure
        END IF;
        SELECT SYSTEM$WAIT(2);
    END
$$;
