import os
import json
import logging
from typing import Any, Dict, Optional


class JsonLogFormatter(logging.Formatter):
    """
    Formats log records as structured JSON with custom fields.
    """

    def __init__(self, service: Optional[str] = None, *args, **kwargs):
        self.service = service
        super().__init__(*args, **kwargs)

    def format(self, record: logging.LogRecord) -> str:
        # Custom log entry fields
        log_payload: Dict[str, Any] = {
            "log_message": record.getMessage(),
            "service": self.service,
            "file": record.filename,
            "line": record.lineno,
            "function": record.funcName,
            "level": record.levelname,
            "logger": record.name,
        }

        # Standard LogRecord attributes to exclude from extra
        reserved: set[str] = {
            "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
            "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
            "created", "msecs", "relativeCreated", "thread", "threadName",
            "processName", "process"
        }

        # Add any extra fields passed via logger
        extra: Dict[str, Any] = {
            k: v for k, v in record.__dict__.items() if k not in reserved
        }
        if extra:
            log_payload.update(extra)

        return json.dumps(log_payload, default=str)


def configure_logging(
    service_name: str,
    level: Optional[str] = None,
    use_cloud_logging: bool = True
) -> None:
    """
    Configures logging for the application with JSON formatting and optional Cloud Logging.

    Args:
        service_name (str): Name of the service or application.
        level (Optional[str]): Logging level (e.g., "DEBUG", "INFO").
        use_cloud_logging (bool): Whether to enable Google Cloud Logging handler.
    """
    # Set log level based on environment or argument
    environment = os.environ.get("ENVIRONMENT", "LOCAL")
    default_level = "DEBUG" if environment == "LOCAL" else "INFO"
    log_level = level or default_level

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Remove all existing handlers
    root_logger.handlers.clear()

    # Add Google Cloud Logging handler if enabled
    if use_cloud_logging:
        try:
            import google.cloud.logging
            from google.cloud.logging.handlers import CloudLoggingHandler

            client = google.cloud.logging.Client()
            cloud_handler = CloudLoggingHandler(client)
            cloud_handler.setLevel(log_level)
            root_logger.addHandler(cloud_handler)
        except ImportError:
            root_logger.warning("google-cloud-logging not installed; skipping cloud handler.")

    # Add a StreamHandler with our JSON formatter
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(log_level)
    stream_handler.setFormatter(JsonLogFormatter(service=service_name))
    root_logger.addHandler(stream_handler)

    # Example debug log for setup completion
    root_logger.debug(
        f"Logger configured for {service_name}",
        extra={"env": environment, "cloud_logging": use_cloud_logging}
    )
