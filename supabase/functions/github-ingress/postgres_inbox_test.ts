import { assert, assertEquals } from "jsr:@std/assert@1.0.14";
import { createLazyPostgresPool } from "./postgres_inbox.ts";

Deno.test("database pool requires one explicit non-fallback TLS mode", () => {
  const pool = createLazyPostgresPool(
    "postgresql://ingress_user:p%40ssword@db.example.test:6543/postgres?sslmode=require",
  );
  assertEquals(pool.size, 0);

  for (
    const connectionString of [
      "postgresql://ingress_user:sentinel-password@db.example.test:6543/postgres",
      "postgresql://ingress_user:sentinel-password@db.example.test:6543/postgres?sslmode=prefer",
      "postgresql://ingress_user:sentinel-password@db.example.test:6543/postgres?sslmode=disable",
      "postgresql://ingress_user:sentinel-password@db.example.test:6543/postgres?sslmode=require&sslmode=disable",
    ]
  ) {
    try {
      createLazyPostgresPool(connectionString);
      throw new Error("unsafe_database_url_was_accepted");
    } catch (error) {
      assert(error instanceof Error);
      assert(!error.message.includes("sentinel-password"));
      assert(!error.message.includes("db.example.test"));
    }
  }
});

Deno.test("database URL parser rejects unsupported query parameters without echoing them", () => {
  try {
    createLazyPostgresPool(
      "postgresql://user:sentinel-password@db.example.test/postgres?sslmode=require&token=sentinel-param",
    );
    throw new Error("unexpected_query_parameter_was_accepted");
  } catch (error) {
    assert(error instanceof Error);
    assertEquals(error.message, "ingress_database_url_invalid");
    assert(!error.message.includes("sentinel-password"));
    assert(!error.message.includes("sentinel-param"));
  }
});
