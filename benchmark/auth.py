import hashlib
import requests
import logging


LOG = logging.getLogger(__name__)


def get_session(username, password, email='', url='http://localhost:8081/auth/v1/login'):
    return _auth_with_payload(email, username, password, url)


def register_user(email, username, password, url='http://localhost:8081/auth/v1/register'):
    session_params = _auth_with_payload(email, username, password, url)
    LOG.info(session_params)
    if int(session_params['http_status_code']) != 200:
        return get_session(username, password, email)
    return session_params


def _auth_with_payload(email, username, password, url):
    session = requests.session()
    hashed_pass = hashlib.md5()
    hashed_pass.update(password)
    auth_response = session.post(url, json={
        'email': email,
        'password': hashed_pass.hexdigest(),
        'username': username
    })
    if auth_response.json().get('user'):
        return {
            'session': session,
            'user_id': auth_response.json()['user']['id'],
            'http_status_code': auth_response.status_code
        }
    return {
        'session': session,
        'http_status_code': auth_response.status_code
    }
