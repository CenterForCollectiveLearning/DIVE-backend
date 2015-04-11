from api.api import app

PORT = 8888

if __name__ == '__main__':
    app.debug = True
    app.run(port=PORT)