from dive.base.core import create_app
from dive.server.core import create_api

if __name__ == '__main__':
    app = create_app()
    api = create_api(app)
    app.run(host=app.config['HOST'], port=app.config['PORT'], threaded=True)
