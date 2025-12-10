from typing import Any
from fif.com.sfa.dataflow.session_events.infrastructure.port.cypher_port import CipherPort

class CipherService:
    def __init__(self, cipher_port: CipherPort):
        self._cipher = cipher_port

    def _process_value(self, value: Any, fields: list[str], mode: str, charset: str):
        if isinstance(value, dict):
            return self._process_json(value, fields, mode, charset)
        elif isinstance(value, list):
            return [self._process_value(v, fields, mode, charset) for v in value]
        elif isinstance(value, (str, int, float)):
            return self._encrypt_or_decrypt(value, mode)
        else:
            return value

    def _encrypt_or_decrypt(self, val: Any, mode: str) -> Any:
        val_str = str(val)
        if mode == "encrypt":
            return self._cipher.encrypt(val_str)
        elif mode == "decrypt":
            return self._cipher.decrypt(val_str)
        return val

    def _process_json(self, data: dict, fields: list[str], mode: str, charset: str) -> dict:
        result = data.copy()
        self._cipher.charset = charset

        for key, value in result.items():
            if key in fields and value is not None:
                result[key] = self._encrypt_or_decrypt(value, mode)
            elif isinstance(value, (dict, list)):
                result[key] = self._process_value(value, fields, mode, charset)
        return result

    def process_json(self, data: dict, fields: list[str], mode: str, charset: str = "utf-8") -> dict:
        return self._process_json(data, fields, mode, charset)
