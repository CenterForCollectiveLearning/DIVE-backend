import hashlib
import requests
import logging


LOG = logging.getLogger(__name__)


def get_session(username, password, url='http://localhost:8081/auth/v1/login'):
    session = requests.session()
    hashed_pass = hashlib.md5()
    hashed_pass.update(password)
    session.post(url, json={
        "email": "",
        "password": hashed_pass.hexdigest(),
        "rememberMe": "true",
        "username": username
    })
    return session
