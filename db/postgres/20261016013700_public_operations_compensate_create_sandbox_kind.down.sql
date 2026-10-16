SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Narrowing back refuses while a `compensate_create_sandbox` operation is still
-- queued or running: the dispatcher updates the row on every claim and result,
-- and each of those updates would fail against the narrowed CHECK, leaving the
-- guest it exists to tear down with no owner. Roll back the code that queues
-- the kind first, let the outstanding operations finish, then this migration.
--
-- Terminal rows don't block it. The narrowed CHECK is `NOT VALID`, so existing
-- rows aren't re-checked, and nothing updates a terminal operation; retention
-- deletes them, which a CHECK doesn't evaluate.
--
-- The first check runs before the ACCESS EXCLUSIVE lock, so a refusal doesn't
-- block traffic. The second, identical one runs after the ALTERs, under that
-- lock, so no operation can be queued between the check and the narrowed CHECK.
-- A failed check rolls the whole migration back.
DO $$
BEGIN
    IF EXISTS (SELECT 1
                 FROM public_operations
                WHERE kind = 'compensate_create_sandbox'
                  AND state IN ('queued', 'running')) THEN
        RAISE EXCEPTION 'public_operations has queued or running '
            'compensate_create_sandbox operations; let them finish before '
            'rolling back this migration';
    END IF;
END;
$$;

ALTER TABLE public_operations
    -- squawk-ignore ban-drop-constraint
    DROP CONSTRAINT IF EXISTS public_operations_kind_check;

ALTER TABLE public_operations
    ADD CONSTRAINT public_operations_kind_check
        CHECK (kind IN (
            'create_sandbox', 'fork_sandbox', 'update_sandbox', 'delete_sandbox',
            'start_sandbox', 'stop_sandbox', 'pause_sandbox', 'resume_sandbox',
            'suspend_sandbox', 'restore_sandbox', 'wake_sandbox', 'kill_sandbox',
            'checkpoint_sandbox', 'enable_sandbox_snapshots',
            'disable_sandbox_snapshots', 'rollback_sandbox', 'delete_template',
            'create_session'
        ))
        NOT VALID;

SET LOCAL statement_timeout = '1s';

DO $$
BEGIN
    IF EXISTS (SELECT 1
                 FROM public_operations
                WHERE kind = 'compensate_create_sandbox'
                  AND state IN ('queued', 'running')) THEN
        RAISE EXCEPTION 'public_operations has queued or running '
            'compensate_create_sandbox operations; let them finish before '
            'rolling back this migration';
    END IF;
END;
$$;
