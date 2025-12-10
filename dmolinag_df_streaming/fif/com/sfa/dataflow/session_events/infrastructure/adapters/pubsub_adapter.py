import apache_beam as beam
import base64
import json
import re


class PubSubAdapter(beam.PTransform):
    def __init__(self, subscription: str, with_attributes: bool = False):
        super().__init__()
        self.subscription = subscription
        self.with_attributes = with_attributes

    def expand(self, pcoll):
        if self.with_attributes:
            messages = (
                    pcoll
                    | "Read from Pub/Sub (with attributes)" >>
                    beam.io.ReadFromPubSub(subscription=self.subscription, with_attributes=True)
                    | "Decode JSON with attributes" >>
                    beam.Map(self._decode_with_attributes)
            )
        else:
            messages = (
                    pcoll
                    | "Read from Pub/Sub" >>
                    beam.io.ReadFromPubSub(subscription=self.subscription)
                    | "Decode JSON" >>
                    beam.Map(self._decode_message)
            )
        return messages

    # -------------------------------------------------------------------------
    @staticmethod
    def _decode_message(message_bytes):
        try:

            data_str = message_bytes.decode("utf-8")
            if not data_str.strip().startswith("{"):
                data_str = base64.b64decode(data_str).decode("utf-8")

            cleaned = (
                data_str
                .replace("NULL", "null")
                .replace("None", "null")
                .replace("NaN", "null")
                .replace("\n", "")
            )
            # Remove trailing commas before closing brace
            cleaned = re.sub(r",\s*}", "}", cleaned)

            # Parse JSON safely
            record = json.loads(cleaned)
            print(f" Received message: {record}")
            return record

        except Exception as e:
            print(f"Error decoding Pub/Sub message: {e}")
            try:
                print(f" Raw content:\n{data_str}")
            except Exception:
                pass
            return None

    # -------------------------------------------------------------------------
    @staticmethod
    def _decode_with_attributes(msg):
        try:

            data_str = msg.data.decode("utf-8")

            if not data_str.strip().startswith("{"):
                data_str = base64.b64decode(data_str).decode("utf-8")

            cleaned = (
                data_str
                .replace("NULL", "null")
                .replace("None", "null")
                .replace("NaN", "null")
                .replace("\n", "")
            )
            cleaned = re.sub(r",\s*}", "}", cleaned)
            data = json.loads(cleaned)

            attributes = dict(msg.attributes)
            print(f" Message: {data} | Attributes: {attributes}")

            return {"data": data, "attributes": attributes}

        except Exception as e:
            print(f" Error decoding Pub/Sub message with attributes: {e}")
            return None
