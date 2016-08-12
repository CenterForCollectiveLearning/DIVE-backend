import logging

from dive.base.core import create_app

def create_api(app):
    from flask.ext.restful import Api
    from api import add_resources

    api = Api(catch_all_404s=True)
    api = add_resources(api)
    api.init_app(app)

    return api

app = create_app()
app.logger.addHandler(logging.StreamHandler())
app.logger.setLevel(logging.INFO)

api = create_api(app)
