class BaseConfig(object):
    DEBUG = False
    TESTING = False
    UPLOAD_FOLDER = 'uploads'
    PORT = 8081
    RECOMPUTE_FIELD_PROPERTIES = False
    RECOMPUTE_VIZ_SPECS = False

class ProductionConfig(BaseConfig):
    SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://localhost:5432/dive'

class DevelopmentConfig(BaseConfig):
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://localhost:5432/dive'

class TestingConfig(BaseConfig):
    TESTING = True
