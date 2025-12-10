import json
from typing import Union

def crypto_execute(
        cipher_adapter,
        cipher_service,
        data: Union[str, dict],
        mode: str,
        fields: list[str] = None,
        charset: str = "utf-8",
):

    if isinstance(data, str):
        # Determinar si es texto o JSON serializado
        try:
            parsed = json.loads(data)
            data = parsed
        except json.JSONDecodeError:
            # No es JSON, es texto plano
            if mode == "encrypt":
                return cipher_adapter.encrypt(data)
            elif mode == "decrypt":
                return cipher_adapter.decrypt(data)
            else:
                raise ValueError("El parámetro 'mode' debe ser 'encrypt' o 'decrypt'")

    # Procesamiento de JSON (dict)
    if isinstance(data, dict):
        if not fields:
            raise ValueError("Debe especificar 'fields' al procesar JSON.")
        return cipher_service.process_json(data, fields, mode, charset)

    raise TypeError("El parámetro 'data' debe ser str o dict válido.")
