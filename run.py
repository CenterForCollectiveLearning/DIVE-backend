from dive.core import create_api


if __name__ == '__main__':
    app = create_api()
    app.run(port=app.config['PORT'])
