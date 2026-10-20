"""Explicit, idempotent private-object cleanup for retention workers."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

from continuum_api.connectivity.storage import StorageFailure, SupabaseStorage
from continuum_api.runtime.retention import DeleteOutcome


class StorageDeletionAdapter:
    def __init__(self, storage_factory: Callable[[], SupabaseStorage]) -> None:
        self._storage_factory = storage_factory

    def delete(self, sink: str, reference_id: str, provider: str | None) -> DeleteOutcome:
        if sink != "object" or provider is not None:
            return DeleteOutcome.UNSUPPORTED
        return asyncio.run(self._delete_object(reference_id))

    async def _delete_object(self, object_key: str) -> DeleteOutcome:
        storage = self._storage_factory()
        try:
            await storage.check_private_bucket()
            try:
                await storage.verify_absent(object_key)
                return DeleteOutcome.DELETED
            except StorageFailure as error:
                if error.code != "delete_not_confirmed":
                    return DeleteOutcome.UNKNOWN
            try:
                await storage.delete(object_key)
            except StorageFailure:
                # Deletion may have succeeded despite response loss. Observe it.
                pass
            await storage.verify_absent(object_key)
            return DeleteOutcome.DELETED
        except StorageFailure:
            return DeleteOutcome.UNKNOWN
        finally:
            await storage.aclose()
