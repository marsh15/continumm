-- Install after migration 20261020_26 and the recovery Edge Function are deployed.
-- The database issues a one-use ticket so the cron job stores no bearer secret.
-- The 110s request limit stays below Supabase Edge's 150s free wall limit.

SELECT cron.schedule(
    'continumm-github-delivery-recovery',
    '*/5 * * * *',
    $recovery_job$
        WITH ticket AS MATERIALIZED (
            SELECT ticket_id, ticket_value
            FROM recovery_private.issue_scan_ticket()
        )
        SELECT net.http_post(
            url := 'https://<project-ref>.supabase.co/functions/v1/github-delivery-recovery',
            headers := '{"content-type":"application/json"}'::jsonb,
            body := jsonb_build_object(
                'ticket_id', ticket.ticket_id,
                'ticket', ticket.ticket_value
            ),
            timeout_milliseconds := 110000
        )
        FROM ticket;
    $recovery_job$
);
