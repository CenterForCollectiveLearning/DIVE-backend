from flask import abort
import datetime
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from haikunator import Haikunator
from flask_login import current_user

from dive.base.serialization import jsonify
from dive.base.core import db, login_manager 
from dive.base.constants import Role, ModelName, AuthStatus, AuthMessage, AuthErrorType
from dive.base.db.helpers import row_to_dict
from dive.base.db.models import *

import logging
logger = logging.getLogger(__name__)


def project_auth(project_id):
    matching_project = ProjectModel.get_one(id=project_id)

    authorized = False
    message = {}
    status = 401
    if is_authorized_user(current_user, matching_project):
        authorized = True
        status = 200
    else:
        message = {
            'status': 'error',
            'message': 'Not authorized'
        }
    return authorized, jsonify(message, status=status)


def is_authorized_user(current_user, matching_project):
    if current_user.is_global_admin() or not matching_project.private:
        return True

    if matching_project.private:
        return (matching_project.user_id == current_user.id)


def logged_in():
    return current_user.is_authenticated and current_user.is_active()


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


@login_manager.user_loader
def load_account(user_id):
    return UserModel._get_one_object(id=user_id)


def check_email_exists(email):
    user = UserModel._get_one_object(email=email)
    if user:
        return user
    else:
        return False

def validate_registration(username, email):
    matching_email = UserModel._get_one_object(email=email)
    result = {}
    if matching_email:
        result['email'] = 'Account with e-mail already exists'

    matching_username = UserModel.query.filter_by(username=username).first()
    if matching_username:
        result['username'] = 'Account with username already exists'

    if matching_email or matching_username:
        return result, False

    return 'Valid registration', True


def team_exists(team_name):
    return TeamModel.get_count(name=team_name)

def create_team(team_name):
    t = TeamModel.create(name=team_name)

def confirm_user(**kwargs):
    return UserModel.confirm_user(**kwargs)

def get_user(**kwargs):
    return UserModel._get_one_object(**kwargs)


haikunator = Haikunator()
def create_anonymous_user():
    user = User.create(
        username=haikunator.haikunate(),
        email='',
        password='',
        confirmed=None,
        anonymous=True
    )
    return user


def register_user(username, email, password, user_id=None, confirmed=True, anonymous=False, admin=[], teams=[], create_teams=True):
    if user_id:
        user = UserModel._get_one_object(user_id)
        setattr(user, 'username', username)
        setattr(user, 'email', email)
        setattr(user, 'password', password)
        setattr(user, 'confirmed', confirmed)
        setattr(user, 'anonymous', anonymous)
    else:
        user = UserModel.create(
            username=username,
            email=email,
            password=password,
            confirmed=confirmed,
            anonymous=anonymous
        )
    if admin:
        for admin_team_name in admin:
            if team_exists(admin_team_name):
                t = TeamModel.query.filter_by(name=admin_team_name).one()
            else:
                if create_teams:
                    t = TeamModel(name=admin_team_name)
                    db.session.add(t)
                    db.session.commit()
            if t:
                user.admin.append(t)
    if teams:
        for team_name in teams:
            if team_exists(team_name):
                t = TeamModel.query.filter_by(name=team_name).one()
            else:
                if create_teams:
                    t = TeamModel(name=team_name)
                    db.session.add(t)
                    db.session.commit()
            if t:
                user.teams.append(t)

    db.session.add(user)
    db.session.commit()
    return user  # Not turning to dictionary because of flask-login

def change_user_password_by_email(email, password):
    try: user = UserModel.query.filter_by(email=email).one()
    except NoResultFound, e: return None
    except MultipleResultsFound, e: raise e

    user.password = password
    user.confirmed_on = datetime.datetime.now()
    db.session.commit()

    return user

def delete_user(user_id, password, name=None):
    try:
        user = UserModel.query.filter_by(id=user_id, password=password).one()
    except NoResultFound, e:
        return None
    except MultipleResultsFound, e:
        raise e
    db.session.delete(user)
    db.session.commit()
    return user

def check_user_auth(password, email=None, username=None):
    '''
    Returns object { 'status': str, 'message': str, 'user': obj}

    TODO: How to best structure this?
    TODO: Multiple returns?
    '''
    status = AuthStatus.SUCCESS.value
    message = ''
    user = None
    error_type = None

    if not (email or username):
        status = AuthStatus.ERROR.value
    else:
        if email:
            if UserModel.query.filter_by(email=email).count():
                user = UserModel.query.filter_by(email=email, password=password).first()
            else:
                result = {
                    'user': None,
                    'status': AuthStatus.ERROR.value,
                    'message': AuthMessage.EMAIL_NOT_FOUND.value,
                    'error_type': AuthErrorType.EMAIL.value
                }
                return result

        if username:
            if UserModel.query.filter_by(username=username).count():
                user = UserModel.query.filter_by(username=username, password=password).first()
            else:
                result = {
                    'user': None,
                    'status': AuthStatus.ERROR.value,
                    'message': AuthMessage.USERNAME_NOT_FOUND.value,
                    'error_type': AuthErrorType.USERNAME.value
                }
                return result

        if not user:
            message = AuthMessage.INCORRECT_CREDENTIALS.value
            error_type = AuthErrorType.GENERAL.value
            status = AuthStatus.ERROR.value

    return {
        'status': status,
        'message': message,
        'user': user,
        'error_type': error_type
    }


def delete_anonymous_data(user_id):
    user = UserModel.query.get_or_404(user_id)
    db.session.delete(user)
    db.session.commit()
    return user
