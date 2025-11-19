import requests

class EventHook:
    def __init__(self, base_url: str):
        self.base_url = base_url

    def send_event(self, url, api_key, event_data: dict):
        headers = {
            "Content-Type": "application/json",
            "X-API-Key": api_key
        }
        full_url = f"{self.base_url}/{url}"
        try:
            response = requests.post(full_url, headers=headers, json=event_data)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Error sending event: {e}")