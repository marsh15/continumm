-- Install only after migration 20261011_02 and the Edge Function are deployed.
-- Execute as the trusted Supabase database operator (normally `postgres`).
-- This queues an Edge request only when the DB-issued one-use ticket function
-- returns a row. pg_net's request ID means queued, not processed.
-- 140s is below the 150s Edge Free wall limit and above the Edge dispatcher's
-- 100s backend deadline. A late result still needs its DB lease fence.

SELECT cron.schedule(
    'continumm-wake-dispatch',
    '*/5 * * * *',
    $wake_job$
        WITH ticket AS MATERIALIZED (
            SELECT ticket_id, ticket_value
            FROM wake_private.issue_dispatch_ticket()
        )
        SELECT net.http_post(
            url := 'https://<project-ref>.supabase.co/functions/v1/wake-dispatch',
            headers := '{"content-type":"application/json"}'::jsonb,
            body := jsonb_build_object(
                'ticket_id', ticket.ticket_id,
                'ticket', ticket.ticket_value
            ),
            timeout_milliseconds := 140000
        )
        FROM ticket;
    $wake_job$
);
