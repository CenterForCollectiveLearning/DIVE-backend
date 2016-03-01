from dive.core import login_manager
from dive.db.models import User
from dive.db.accounts import is_authorized_user

from flask.ext.login import current_user
from functools import wraps, update_wrapper


def project_auth(project_id):
    if is_authorized_user(current_user.id, project_id):
        return True
    else:
        return False


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
