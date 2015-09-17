class BaseConfig(object):
    DEBUG = False
    TESTING = False
    UPLOAD_FOLDER = 'uploads'

class ProductionConfig(BaseConfig):
    SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://localhost:5432/dive'

class DevelopmentConfig(BaseConfig):
    DEBUG = True

class TestingConfig(BaseConfig):
    TESTING = True
