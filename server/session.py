from api import app, api
from db import MongoInstance as MI

from flask import request, make_response, json
from flask.ext.restful import Resource
from passlib.hash import sha256_crypt

@app.route("/api/register", methods=["POST"])
def register() :
    params = request.json['params']

    existing = MI.getUser({'userName' : params['userName']})
    resp = {}

    if (len(existing) != 1) :
        pw_hash = sha256_crypt.encrypt(params['password'])
        uID = MI.postNewUser(params['userName'], params['displayName'], pw_hash)
        resp['success'] = 1
        resp['user'] = {
            'userName' : params['userName'],
            'displayName' : params['displayName'],
            'uID' : uID
        }
    else :
        resp['success'] = 0

    return make_response(json.jsonify(resp))

@app.route("/api/login", methods=["GET"])
def login() :
    params = request.args

    user = MI.getUser({'userName' : params['userName']})
    resp = {}
    if (len(user) != 1) :
        resp['success'] = 0
    else :
        u = user[0]

        if sha256_crypt.verify(params['password'], u['password']) :
            resp['success'] = 1
            resp['user'] = {
                'userName' : u['userName'],
                'displayName' : u['displayName'],
                'uID' : u['uID']
            }
        else :
            resp['success'] = 0

    return make_response(json.jsonify(resp))
