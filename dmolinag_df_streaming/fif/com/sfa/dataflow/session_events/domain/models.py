from __future__ import annotations
from typing import Any, Dict, List, Optional, Union
from enum import Enum
from pydantic import BaseModel, ConfigDict, field_validator, model_validator


class EncryptionMode(str, Enum):
    encrypt = "encrypt"
    decrypt = "decrypt"


class CipherConfig(BaseModel):
    key: str
    iv: str
    charset: str = "utf-8"

    @model_validator(mode="after")
    def validate_lengths(self):
        key_len = len(self.key.encode(self.charset))
        iv_len = len(self.iv.encode(self.charset))
        if key_len not in (16, 24, 32):
            raise ValueError(
                f"key debe tener 16/24/32 bytes; actuales: {key_len} (charset={self.charset})"
            )
        if iv_len != 16:
            raise ValueError(
                f"iv debe tener 16 bytes; actuales: {iv_len} (charset={self.charset})"
            )
        return self


class FieldRule(BaseModel):
    name: str
    action: Optional[EncryptionMode] = None  # None => usa el modo global

    @field_validator("name")
    @classmethod
    def non_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("El nombre del campo no puede estar vacío")
        return v


class ProcessSpec(BaseModel):
    mode: EncryptionMode
    fields: List[Union[str, FieldRule]]

    @model_validator(mode="after")
    def coerce_fields(self):
        coerced: List[FieldRule] = []
        for f in self.fields:
            if isinstance(f, str):
                coerced.append(FieldRule(name=f))
            elif isinstance(f, FieldRule):
                coerced.append(f)
            else:
                raise TypeError("fields debe contener strings o FieldRule")
        if not coerced:
            raise ValueError("Debe indicar al menos un campo a procesar")
        self.fields = coerced
        return self
