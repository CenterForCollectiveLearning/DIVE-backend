from dive.core import create_app, create_api
import logging

app = create_app()
app.logger.addHandler(logging.StreamHandler())
app.logger.setLevel(logging.INFO)

api = create_api(app)
