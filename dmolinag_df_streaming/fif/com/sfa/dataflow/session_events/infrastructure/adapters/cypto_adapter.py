from Cryptodome.Cipher import AES
from Cryptodome.Util.Padding import pad, unpad
import base64, re

class AESCipherAdapter:
    def __init__(self, key: str, iv: str, charset: str = "utf-8"):
        self.charset = charset
        self.key = key.encode(charset)
        self.iv = iv.encode(charset)
        if len(self.key) not in (16, 24, 32):
            raise ValueError("La clave AES debe tener 16, 24 o 32 bytes.")
        if len(self.iv) != 16:
            raise ValueError("El vector IV debe tener exactamente 16 bytes.")

    def encrypt(self, plaintext: str) -> str:
        cipher = AES.new(self.key, AES.MODE_CBC, self.iv)
        padded = pad(plaintext.encode(self.charset), AES.block_size)
        encrypted = cipher.encrypt(padded)
        return base64.b64encode(encrypted).decode(self.charset)

    def decrypt(self, encrypted_text: str) -> str:
        cipher = AES.new(self.key, AES.MODE_CBC, self.iv)
        decrypted = cipher.decrypt(base64.b64decode(encrypted_text))
        try:
            unpadded = unpad(decrypted, AES.block_size)
        except ValueError:
            unpadded = decrypted
        clean_text = re.sub(r"[\x00-\x1F]+", "", unpadded.decode(self.charset, errors="ignore"))
        return clean_text
