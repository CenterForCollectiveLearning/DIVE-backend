import json
import requests
import logging


LOG = logging.getLogger(__name__)


def get_session(username, password, url='http://localhost:8081/auth/v1/login'):
    session = requests.session()
    session.post(url, json={"email": "", "password": password, "rememberMe": "true", "username": username})
    return session
