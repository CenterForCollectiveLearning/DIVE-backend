from flask import abort
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound

from dive.core import db, login_manager
from dive.db import ModelName, row_to_dict
from dive.db.models import User, Project


@login_manager.user_loader
def load_account(user_id):
    return User.query.get(user_id)


def is_authorized_user(user_id, project_id):
    matching_project = Project.query.get(project_id)
    print matching_project.private
    if matching_project.private:
        if matching_project.user_id is user_id:
            return True
        else:
            return False
    else:
        return True


def validate_registration(username, email):
    matching_email = User.query.filter_by(email=email).first()
    if matching_email:
        return 'Matching e-mail', False

    matching_username = User.query.filter_by(username=email).first()
    if matching_username:
        return 'Matching username', False

    return 'Valid registration', True


def register_user(username, email, password, name=None):
    user = User(
        username=username,
        email=email,
        password=password,
        name=name
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
    return row_to_dict(user)


def check_user_auth(password, email=None, username=None):
    if not (email or username):
        return 'Need to provide either username or email', False
    user = None
    if email:
        user = User.query.filter_by(email=email, password=password).first()
    elif username:
        user = User.query.filter_by(username=username, password=password).first()
    if user:
        return user, True
    else:
        return None, False
