from __future__ import annotations
from hooks.event_hook import EventHook
from airflow.models import Variable
import logging
import time
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def send_weather_alert(api_url, alert_data: dict = {}, **context) -> None:
    """
    Sends weather alert data to an external alerting service.

    Args:
        alert_data: A dictionary containing the alert information.
    """
    if context is None:
        context = {}
        
    alert_data = {
        "event_type": "weather_alert",
        "source": "de-pipeline"
    }
    alert_service_url = Variable.get("ALERT_SERVICE_BASE_URL", default_var=None)
    api_key = Variable.get("ALERT_SERVICE_API_KEY", default_var=None)

    if not alert_service_url or not api_key:
        logger.error("Alert service URL or API key is not set.")
        return

    event_hook = EventHook(alert_service_url)
    try:
        event_hook.send_event(api_url, api_key, alert_data)
        logger.info("Weather alert sent successfully to %s", api_url)
    except requests.RequestException as e:
        logger.error("Failed to send weather alert: %s", e)