from dive.base.core import login_manager
from dive.base.db.models import User
from dive.base.db.accounts import is_authorized_user
from dive.base.serialization import jsonify

from flask.ext.login import current_user
from functools import wraps, update_wrapper


def project_auth(project_id):
    if is_authorized_user(current_user, project_id):
        return True, None
    else:
        return False, jsonify({
            'status': 'error',
            'message': 'Not authorized'
        }, status=401)


def logged_in():
    return current_user.is_authenticated() and current_user.is_active()


def is_admin():
    return logged_in() and current_user.admin


def create():
    return True


def read(account):
    return True


def update(account):
    return logged_in()


def delete(account):
    return False
