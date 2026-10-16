SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Admit `'compensate_create_sandbox'` as a public operation kind.
--
-- A create that started a guest and then cannot keep it queues this operation
-- to tear the guest down. It is a kind of its own, not a `delete_sandbox` with
-- a marker, because claims select by kind: a cluster-controller that predates
-- the kind never claims one. Nothing writes the value until the API change that
-- produces it is deployed; until then this widening is inert.
--
-- Widening only, and `NOT VALID` like the baseline constraint it replaces. The
-- drop is replaced in the same transaction by a strictly wider CHECK, so the
-- only guarantee it removes is that no row carries the new kind.
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
            'create_session',
            'compensate_create_sandbox'
        ))
        NOT VALID;
