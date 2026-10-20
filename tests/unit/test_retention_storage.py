"""Private-object deletion is observed before releasing storage capacity."""

from __future__ import annotations

import httpx
import pytest

from continuum_api.connectivity.storage import SupabaseStorage
from continuum_api.runtime.retention import DeleteOutcome
from continuum_api.runtime.retention_storage import StorageDeletionAdapter


@pytest.mark.parametrize(
    "initially_present,lost_response", [(True, False), (True, True), (False, False)]
)
def test_storage_cleanup_reconciles_presence_and_lost_delete_response(
    initially_present: bool,
    lost_response: bool,
) -> None:
    present = initially_present
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal present
        requests.append(request)
        if "/bucket/" in request.url.path:
            return httpx.Response(
                200,
                json={
                    "id": "continuum-checkpoints",
                    "name": "continuum-checkpoints",
                    "public": False,
                },
            )
        if request.method == "DELETE":
            present = False
            if lost_response:
                raise httpx.ReadError("synthetic response loss")
            return httpx.Response(200, json=[])
        return httpx.Response(200 if present else 404, content=b"object" if present else b"")

    adapter = StorageDeletionAdapter(
        lambda: SupabaseStorage(
            "https://fixture.supabase.co",
            "sb_secret_synthetic_only",
            "continuum-checkpoints",
            transport=httpx.MockTransport(handler),
        )
    )
    assert adapter.delete("object", "checkpoints/fixture.tar.gz", None) is DeleteOutcome.DELETED
    assert sum(request.method == "DELETE" for request in requests) == int(initially_present)
    assert all(request.url.host == "fixture.supabase.co" for request in requests)


def test_provider_copy_deletion_is_not_invented() -> None:
    def no_storage() -> SupabaseStorage:
        raise AssertionError("provider cleanup must not use the object credential")

    assert (
        StorageDeletionAdapter(no_storage).delete("provider", "opaque-id", "openai")
        is DeleteOutcome.UNSUPPORTED
    )
