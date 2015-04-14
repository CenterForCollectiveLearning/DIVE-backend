from api.api import app
from flask import request

@app.before_request
def option_autoreply():

    """ Always reply 200 on OPTIONS request """
    if request.method == 'OPTIONS':
        resp = app.make_default_options_response()

        print "Here comes an OPTIONS request"

        headers = None
        if 'ACCESS_CONTROL_REQUEST_HEADERS' in request.headers:
            headers = request.headers['ACCESS_CONTROL_REQUEST_HEADERS']

        h = resp.headers

        # Allow the origin which made the XHR
        h['Access-Control-Allow-Origin'] = request.headers['Origin']
        # Allow the actual method
        h['Access-Control-Allow-Methods'] = request.headers['Access-Control-Request-Method']
        # Allow for 10 seconds
        h['Access-Control-Max-Age'] = "10"

        # We also keep current headers
        if headers is not None:
            h['Access-Control-Allow-Headers'] = headers

        return resp


@app.after_request
def replace_nan(resp):
    print resp
    try:
        cleaned_data = resp.get_data().replace('nan', 'null').replace('NaN', 'null')
        resp.set_data(cleaned_data)
        return resp
    except:
        return resp


@app.after_request
def set_allow_origin(resp):
    """ Set origin for GET, POST, PUT, DELETE requests """

    h = resp.headers

    # Allow crossdomain for other HTTP Verbs
    if request.method != 'OPTIONS' and 'Origin' in request.headers:
        h['Access-Control-Allow-Origin'] = request.headers['Origin']
    return resp

@app.route('/')
def index():
    return app.send_static_file('index.html')

PORT = 8888

if __name__ == '__main__':
    app.debug = True
    app.run(port=PORT)