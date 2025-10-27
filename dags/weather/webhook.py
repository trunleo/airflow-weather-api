"""Weather notification webhook service for bulk notification API."""

import json
from datetime import UTC, datetime
from typing import Any

import requests
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook

from weather.alert_service import WeatherAlertService


class WebhookService:
    """Webhook notification service for bulk weather alerts.

    This service handles webhook payload building, authentication,
    and delivery for bulk notification API with batching support.

    Attributes:
        postgres_conn_id: Airflow Postgres connection ID.
        webhook_url: Full webhook URL for notification delivery.
        weather_webkook_key: API key for webhook authentication.
        webhook_secret: Secret key for HMAC SHA256 signature.
        alert_service: Weather alert detection service.
        batch_size: Maximum number of items per batch.
    """

    def __init__(
        self,
        postgres_conn_id: str = "WEATHER_POSTGRES_CONN",
        webhook_url: str | None = None,
        weather_webkook_key: str | None = None,
        webhook_secret: str | None = None,
        batch_size: int = 50,
    ) -> None:
        """Initialize webhook service.

        Args:
            postgres_conn_id: Airflow Postgres connection ID.
            webhook_url: Full webhook URL for notifications.
            weather_webkook_key: API key for authentication.
            webhook_secret: Secret key for payload signing.
            batch_size: Maximum items per batch (default: 50).
        """
        self.postgres_conn_id = postgres_conn_id
        self.webhook_url = webhook_url
        self.weather_webkook_key = weather_webkook_key
        self.webhook_secret = webhook_secret
        self.batch_size = batch_size
        self.alert_service = WeatherAlertService(
            postgres_conn_id=postgres_conn_id
        )


    def _build_headers(self, payload_json: str) -> dict[str, str]:
        """Build HTTP headers with authentication and signature.

        Args:
            payload_json: JSON string of the payload to sign.

        Returns:
            dict[str, str]: HTTP headers dictionary.
        """
        headers = {
            "Content-Type": "application/json",
        }
        if self.weather_webkook_key:
            headers["X-API-Key"] = self.weather_webkook_key
        if self.webhook_secret:
            headers["X-Signature"] = self.webhook_secret

        return headers

    def _ensure_webhook_response_table(self) -> None:
        """Create webhook_responses table if it doesn't exist.

        Table structure for bulk webhook responses:
            - batch_id: Unique batch identifier (PRIMARY KEY)
            - code: HTTP response code
            - message: Response message
            - total: Total number of items in batch
            - success_count: Number of successful items
            - failure_count: Number of failed items
            - results: Per-item results array (JSONB)
            - created_at: Timestamp of response
        """
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS webhook_responses (
            batch_id TEXT PRIMARY KEY,
            code INTEGER,
            message TEXT,
            total INTEGER,
            success_count INTEGER,
            failure_count INTEGER,
            results JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        hook.run(create_table_sql)

    def _save_webhook_response(
        self,
        batch_id: str,
        response_body: dict[str, Any] | None,
    ) -> None:
        """Save bulk webhook response to database.

        Args:
            batch_id: Unique batch identifier from request.
            response_body: Response with code, message, total,
                success/failure counts, results.
        """
        if not response_body:
            return

        # Extract fields from bulk response
        code = response_body.get("code")
        message = response_body.get("message")
        total = response_body.get("total")
        success_count = response_body.get("success_count")
        failure_count = response_body.get("failure_count")
        results = response_body.get("results", [])

        # Use UPSERT to handle duplicates
        insert_sql = """
        INSERT INTO webhook_responses (
            batch_id, code, message, total, success_count, failure_count, results
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (batch_id) DO UPDATE SET
            code = EXCLUDED.code,
            message = EXCLUDED.message,
            total = EXCLUDED.total,
            success_count = EXCLUDED.success_count,
            failure_count = EXCLUDED.failure_count,
            results = EXCLUDED.results,
            created_at = CURRENT_TIMESTAMP
        """

        parameters = (
            batch_id,
            code,
            message,
            total,
            success_count,
            failure_count,
            json.dumps(results),
        )

        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        hook.run(insert_sql, parameters=parameters)

    def _send_single_batch(
        self,
        batch_id: str,
        items: list[dict[str, Any]],
        timestamp: str,
    ) -> dict[str, Any] | None:
        """Send a single batch of webhook items.

        Args:
            batch_id: Unique batch identifier.
            items: List of webhook items to send.
            timestamp: Batch timestamp.

        Returns:
            dict[str, Any] | None: Response body or None if failed.

        Raises:
            requests.exceptions.RequestException: If request fails.
        """
        payload = {
            "batch_id": batch_id,
            "timestamp": timestamp,
            "items": items,
        }
        payload_json = json.dumps(payload, default=str)
        headers = self._build_headers(payload_json)

        response = requests.post(
            self.webhook_url,
            headers=headers,
            data=payload_json,
            timeout=30,
        )

        try:
            response_body = response.json()
        except json.JSONDecodeError:
            response_body = None

        response.raise_for_status()
        return response_body

    def send_webhook(
        self,
        run_date: str,
        event_type: str = "weather_alert",
        source: str = "de-pipeline",
    ) -> None:
        """Send bulk weather notification webhook with batching.

        Detects alerts and sends them in batches to avoid overloading
        the webhook API. Each batch respects the batch_size limit.

        This method:
        - Detects all weather alerts
        - Splits alerts into batches (default 50 items/batch)
        - Sends each batch separately with authentication
        - Logs all responses to database

        Args:
            run_date: Execution date in YYYY-MM-DD format.
            event_type: Type of event being sent.
            source: Source identifier for the webhook.

        Raises:
            AirflowException: If webhook URL not configured or fails.
        """
        if not self.webhook_url:
            error_msg = "Webhook URL is not configured"
            print(f"ERROR: {error_msg}")
            raise AirflowException(error_msg)

        # Ensure webhook response table exists
        self._ensure_webhook_response_table()

        # Detect all alerts and build items
        timestamp = datetime.now(UTC).isoformat()
        alerts = self.alert_service.detect_all_alerts(run_date=run_date)

        # Transform alerts to webhook items
        all_items = []
        for alert in alerts:
            severity_priority_map = {
                "critical": "HIGH",
                "high": "HIGH",
                "medium": "MEDIUM",
                "low": "LOW",
            }
            priority = severity_priority_map.get(
                alert.get("severity", "medium"),
                "MEDIUM",
            )

            is_heat_wave = alert.get("alert_type") in ["heat_wave"]
            temperature = alert.get("value") if is_heat_wave else None

            item = {
                "event_type": event_type,
                "source": source,
                "timestamp": timestamp,
                "data": {
                    "temperature": temperature,
                    "location": alert.get("province", "unknown"),
                    "alert_type": alert.get("alert_type"),
                    "severity": alert.get("severity"),
                    "message": alert.get("message"),
                    "value": alert.get("value"),
                    "unit": alert.get("unit"),
                    "date": alert.get("date"),
                    "latitude": alert.get("latitude"),
                    "longitude": alert.get("longitude"),
                },
                "priority": priority,
                "receiver_ids": [],
                "metadata": {
                    "trace_id": f"weather-{run_date}-{timestamp}",
                    "alert_type": alert.get("alert_type"),
                    "province": alert.get("province"),
                },
            }
            all_items.append(item)

        total_items = len(all_items)
        print(f"Detected {total_items} alerts for date: {run_date}")

        if total_items == 0:
            print("No alerts to send, skipping webhook")
            return

        # Split into batches
        batches = [
            all_items[i : i + self.batch_size]
            for i in range(0, total_items, self.batch_size)
        ]
        total_batches = len(batches)

        print(
            f"Sending {total_items} items in {total_batches} "
            f"batch(es) (max {self.batch_size} items/batch)"
        )

        # Send each batch
        for batch_num, batch_items in enumerate(batches, 1):
            now_str = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
            batch_id = f"weather-batch-{run_date}-{now_str}-{batch_num}"

            try:
                print(
                    f"Sending batch {batch_num}/{total_batches} "
                    f"({len(batch_items)} items)..."
                )

                response_body = self._send_single_batch(
                    batch_id=batch_id,
                    items=batch_items,
                    timestamp=timestamp,
                )

                if response_body:
                    success = response_body.get("success_count", 0)
                    failure = response_body.get("failure_count", 0)
                    print(
                        f"Batch {batch_num} result: "
                        f"{success} success, {failure} failed"
                    )

                    # Save response to database
                    self._save_webhook_response(
                        batch_id=batch_id,
                        response_body=response_body,
                    )

            except requests.exceptions.RequestException as e:
                error_msg = (
                    f"Failed to send batch {batch_num}/{total_batches} "
                    f"for date {run_date}: {e}"
                )
                print(f"ERROR: {error_msg}")
                # Continue with next batch instead of failing completely
                continue

        print(f"Completed sending all batches for date: {run_date}")


# Backward-compatible function wrapper for Airflow DAGs
def send_notification_webhook(
    run_date: str,
    webhook_url: str,
    postgres_conn_id: str = "WEATHER_POSTGRES_CONN",
    weather_webkook_key: str | None = None,
    webhook_secret: str | None = None,
    batch_size: int = 50,
    **kwargs: Any,
) -> None:
    """Send bulk weather notification webhook with batching.

    This is a backward-compatible wrapper for Airflow DAGs.
    Automatically batches alerts to avoid overloading webhook API.

    Args:
        run_date: Execution date in YYYY-MM-DD format.
        webhook_url: Full webhook URL for notification delivery.
        postgres_conn_id: Airflow Postgres connection ID.
        weather_webkook_key: Optional API key for authentication.
        webhook_secret: Optional secret key for payload signing.
        batch_size: Maximum items per batch (default: 50).
        **kwargs: Additional Airflow context parameters.

    Raises:
        AirflowException: If webhook request fails.
    """
    service = WebhookService(
        postgres_conn_id=postgres_conn_id,
        webhook_url=webhook_url,
        weather_webkook_key=weather_webkook_key,
        webhook_secret=webhook_secret,
        batch_size=batch_size,
    )
    service.send_webhook(run_date=run_date)
