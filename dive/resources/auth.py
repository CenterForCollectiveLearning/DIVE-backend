from flask import request, make_response
from flask.ext.restful import Resource, reqparse
from flask.ext.login import current_user, login_user, logout_user

from dive.db import row_to_dict
from dive.db.accounts import validate_registration, register_user, delete_user, check_user_auth
from dive.resources.serialization import jsonify

import logging
logger = logging.getLogger(__name__)


registerPostParser = reqparse.RequestParser()
registerPostParser.add_argument('username', type=str, location='json')
registerPostParser.add_argument('name', type=str, location='json')
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
            account = register_user(username, email, password, name=name)
            login_user(account, remember=True)
            return jsonify(row_to_dict(account))

        else:
            return jsonify({
                'status': 'error',
                'errors': {
                    'message': registration_result
                }
            }, status=400)


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


loginGetParser = reqparse.RequestParser()
loginGetParser.add_argument('username', type=str)
loginGetParser.add_argument('email', type=str)
loginGetParser.add_argument('password', type=str)
class Login(Resource):
    def get(self):
        args = loginGetParser.parse_args()
        username = args.get('username')
        email = args.get('email')
        password = args.get('password')

        user, auth_status = check_user_auth(password, email=email, username=username)
        if auth_status:
            login_user(user, remember=True)
            return jsonify({
                'status': 'success',
                'message': 'Welcome back %s' % user.name
            })
        else:
            return jsonify({
                'status': 'error',
                'message': 'Incorrect username, e-mail, or password'
            }, status=400)


logoutGetParser = reqparse.RequestParser()
logoutGetParser.add_argument('user_name', type=str, location='json')
class Logout(Resource):
    def get(self):
        logout_user()
        return jsonify({
            'status': 'ok',
            'message': 'You have been logged out.'
        })
