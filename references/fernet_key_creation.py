from cryptography.fernet import Fernet
fernet_key = Fernet.generate_key()
# izcg1p9Gh83bLc6nguSpTJXoX-3Boilo46FHwP3CQ7g=

f = Fernet(fernet_key)
token = f.encrypt(b"Probando encriptacion")
print(token)

f.decrypt(token)