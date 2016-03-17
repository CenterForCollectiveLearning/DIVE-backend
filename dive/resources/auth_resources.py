from flask import request, make_response
from flask.ext.restful import Resource, reqparse
from flask.ext.login import current_user, login_user, logout_user
from datetime import timedelta, datetime

from dive.core import login_manager, db
from dive.db import row_to_dict
from dive.db.accounts import validate_registration, register_user, delete_user, check_user_auth
from dive.db.models import User
from dive.resources.serialization import jsonify

import logging
logger = logging.getLogger(__name__)

COOKIE_DURATION = timedelta(days=365)

registerPostParser = reqparse.RequestParser()
registerPostParser.add_argument('username', type=str, location='json')
registerPostParser.add_argument('name', type=str, default='', location='json')
registerPostParser.add_argument('email', type=str, location='json')
registerPostParser.add_argument('password', type=str, location='json')
class Register(Resource):
    def post(self):
        args = registerPostParser.parse_args()
        username = args.get('username')
        name = args.get('name')
        email = args.get('email')
        password = args.get('password')

        registration_result, valid_registration = validate_registration(username, email)
        if valid_registration:
            user = register_user(username, email, password, name=name)
            login_user(user, remember=True)
            response = jsonify({
                'status': 'success',
                'message': 'Welcome to DIVE, %s' % user.username,
                'user': row_to_dict(user)
            })
            response.set_cookie('username', user.username, expires=datetime.utcnow() + COOKIE_DURATION)
            response.set_cookie('email', user.email, expires=datetime.utcnow() + COOKIE_DURATION)
            response.set_cookie('user_id', str(user.id), expires=datetime.utcnow() + COOKIE_DURATION)
            return response

        else:
            return jsonify({
                'status': 'error',
                'message': registration_result
            }, status=401)


userDeleteParser = reqparse.RequestParser()
userDeleteParser.add_argument('user_id', type=str, required=True)
userDeleteParser.add_argument('password', type=str, required=True)
class User(Resource):
    def delete(self):
        args = userDeleteParser.parse_args()
        user_id = args.get('user_id')
        password = args.get('password')

        deleted_user = delete_user(user_id, password)
        return jsonify(deleted_user)


loginPostParser = reqparse.RequestParser()
loginPostParser.add_argument('username', type=str, required=True, location='json')
loginPostParser.add_argument('email', type=str, required=True, location='json')
loginPostParser.add_argument('password', type=str, location='json')
loginPostParser.add_argument('rememberMe', type=bool, default=True, location='json')
class Login(Resource):
    def post(self):
        args = loginPostParser.parse_args()
        username = args.get('username')
        email = args.get('email')
        password = args.get('password')
        rememberMe = args.get('rememberMe')

        user, auth_status = check_user_auth(password, email=email, username=username)
        if auth_status:
            login_user(user, remember=rememberMe)
            if user.username:
                message = 'Welcome back %s!' % user.username
            else:
                message = 'Welcome back!'
            response = jsonify({
                'status': 'success',
                'message': message,
                'user': row_to_dict(user)
            })
            response.set_cookie('username', user.username, expires=datetime.utcnow() + COOKIE_DURATION)
            response.set_cookie('email', user.email, expires=datetime.utcnow() + COOKIE_DURATION)
            response.set_cookie('user_id', str(user.id), expires=datetime.utcnow() + COOKIE_DURATION)
            return response
        else:
            return jsonify({
                'status': 'error',
                'message': {
                    'login': 'Incorrect username, e-mail, or password'
                }
            }, status=401)


logoutPostParser = reqparse.RequestParser()
logoutPostParser.add_argument('username', type=str, location='json')
class Logout(Resource):
    def post(self):
        logout_user()
        response = jsonify({
            'status': 'success',
            'message': 'You have been logged out.'
        })
        response.set_cookie('username', '', expires=0)
        response.set_cookie('email', '', expires=0)
        response.set_cookie('user_id', '', expires=0)
        return response
