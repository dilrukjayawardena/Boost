import polars as pl
from cryptography.fernet import Fernet
import os
import codecs

key_str=os.environ.get("ENCRYPTION_KEY","5349613930632d36354d78492d5154516b75494e516b6b577847357556564a6f47677662714f5233646b413d")
ENCRYPTION_KEY = codecs.decode(key_str.encode('utf-8'), 'hex')
cipher_suite = Fernet(ENCRYPTION_KEY)

def encrypt_value(value: str) -> bytes:
    """Encrypts a single string value using Fernet (AES-128 in CBC mode)."""
    # Fernet works with bytes, so we encode the input string
    return cipher_suite.encrypt(value.encode('utf-8'))


def decrypt_value(value: bytes) -> str:
    """Decrypts a single byte value using Fernet."""
    # Decrypt the bytes and decode back to a string
    return cipher_suite.decrypt(value).decode('utf-8')


