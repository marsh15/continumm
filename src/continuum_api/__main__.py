"""Run the local FastAPI service."""

from __future__ import annotations

import logging

import uvicorn
from pydantic import ValidationError

from continuum_api.connectivity.smoke import maybe_run_startup_smoke, report_json
from continuum_api.logging_config import configure_logging
from continuum_api.settings import get_settings, safe_settings_error


def main() -> None:
    configure_logging("INFO")
    try:
        settings = get_settings()
    except ValidationError as error:
        logging.getLogger(__name__).error(safe_settings_error(error))
        raise SystemExit(2) from None

    configure_logging(settings.log_level)
    smoke_report = maybe_run_startup_smoke()
    if smoke_report is not None:
        result_logger = logging.getLogger(__name__)
        log_method = (
            result_logger.info if smoke_report.status == "completed" else result_logger.warning
        )
        log_method("T03 hosted smoke result %s", report_json(smoke_report))

    uvicorn.run(
        "continuum_api.app:app",
        host=settings.host,
        port=settings.port,
        log_config=None,
        access_log=False,
    )


if __name__ == "__main__":
    main()
