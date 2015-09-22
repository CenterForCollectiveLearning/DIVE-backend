from dive.core import create_app, create_api


if __name__ == '__main__':
    app = create_app()
    api = create_api(app)
    app.run(port=app.config['PORT'])
