from flask import abort
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound

from dive.base.core import db, login_manager
from dive.base.db import ModelName, AuthStatus, AuthMessage, AuthErrorType, row_to_dict
from dive.base.db.models import User, Project
from dive.base.db.constants import Role


@login_manager.user_loader
def load_account(user_id):
    return User.query.get(user_id)


def is_authorized_user(current_user, project_id):
    matching_project = Project.query.get(project_id)
    user_id = current_user.id

    print current_user, project_id

    if current_user.is_admin() or not matching_project.private:
        return True

    if matching_project.private:
        if matching_project.user_id is user_id:
            return True
        else:
            return False


def validate_registration(username, email):
    matching_email = User.query.filter_by(email=email).first()
    result = {}
    if matching_email:
        result['email'] = 'Account with e-mail already exists'

    matching_username = User.query.filter_by(username=username).first()
    if matching_username:
        result['username'] = 'Account with username already exists'

    if matching_email or matching_username:
        return result, False

    return 'Valid registration', True


def register_user(username, email, password, role=Role.USER.value):
    user = User(
        username=username,
        email=email,
        password=password,
        role=role
    )
    db.session.add(user)
    db.session.commit()
    return user  # Not turning to dictionary because of flask-login


def delete_user(user_id, password, name=None):
    try:
        user = User.query.filter_by(id=user_id, password=password).one()
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
            if User.query.filter_by(email=email).count():
                user = User.query.filter_by(email=email, password=password).first()
            else:
                result = {
                    'user': None,
                    'status': AuthStatus.ERROR.value,
                    'message': AuthMessage.EMAIL_NOT_FOUND.value,
                    'error_type': AuthErrorType.EMAIL.value
                }
                return result

        if username:
            if User.query.filter_by(username=username).count():
                user = User.query.filter_by(username=username, password=password).first()
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
