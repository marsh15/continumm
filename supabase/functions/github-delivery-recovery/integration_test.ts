import { Pool } from "@db/postgres";
import { createPostgresRecoveryStore } from "./postgres_recovery.ts";

const databaseUrl = Deno.env.get("CONTINUUM_T39_TEST_DATABASE_URL");
if (!databaseUrl) throw new Error("t39_test_database_url_required");
const pool = new Pool(databaseUrl, 1, true);
const store = createPostgresRecoveryStore(pool);

Deno.test("migration tables persist scan watermark, single-use tickets, and GUID claims", async () => {
  let client: Awaited<ReturnType<typeof pool.connect>> | null = null;
  let stage = "migration_revision";
  try {
    stage = "connect";
    client = await pool.connect();
    stage = "migration_revision";
    const revision = await client.queryObject<{ version_num: string }>`
      SELECT version_num FROM continuum_migrations.alembic_version
    `;
    assert(
      revision.rows[0]?.version_num === "20261020_26",
      "T39 migration is not applied",
    );
    const ticket = await client.queryObject<
      { ticket_id: string; ticket_value: string }
    >`
      SELECT ticket_id::text, ticket_value
      FROM recovery_private.issue_scan_ticket()
    `;
    const value = ticket.rows[0];
    stage = "ticket_issue";
    assert(value !== undefined, "database did not issue a scan ticket");
    client.release();
    client = null;
    assert(
      await store.consumeTicket(value.ticket_id, value.ticket_value),
      "ticket was not consumed",
    );
    assert(
      !(await store.consumeTicket(value.ticket_id, value.ticket_value)),
      "ticket was reusable",
    );

    stage = "scan_lease";
    const initial = await store.beginScan();
    assert(initial !== null, "scan lease was not acquired");
    stage = "delivery_claim";
    assert(
      await store.beginScan() === null,
      "concurrent scanner acquired the same lease",
    );
    const deliveredAt = new Date().toISOString();
    const delivery = {
      id: "1234567890123456789",
      guid: "11111111-1111-4111-8111-111111111111",
      delivered_at: deliveredAt,
      status_code: 500,
    };
    assert(
      await store.claimDelivery(delivery) === "claimed",
      "first GUID claim failed",
    );
    assert(
      await store.claimDelivery(delivery) === "already_requested",
      "GUID was claimed twice",
    );
    await store.recordDeliveryOutcome(delivery.guid, "unknown", null, null);
    assert(
      await store.claimDelivery(delivery) === "already_requested",
      "unknown POST became retryable",
    );
    stage = "watermark";
    await store.completeScan({
      watermark: deliveredAt,
      scannedAt: deliveredAt,
      historyGapSince: null,
      successful: true,
    });
    const state = await store.beginScan();
    assert(state?.watermark === deliveredAt, "watermark did not persist");
    await store.completeScan({
      watermark: deliveredAt,
      scannedAt: deliveredAt,
      historyGapSince: null,
      successful: true,
    });
  } finally {
    console.log(`t39 integration stage: ${stage}`);
    if (client !== null) {
      client.release();
    }
    await pool.end();
  }
});

function assert(condition: boolean, message: string): asserts condition {
  if (!condition) throw new Error(message);
}
