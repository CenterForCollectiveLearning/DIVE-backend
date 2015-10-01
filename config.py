import os

class BaseConfig(object):
    DEBUG = False
    PORT = 8081

    UPLOAD_DIR = os.path.join(os.path.dirname(__file__), 'uploads')
    UPLOAD_DIR = os.path.abspath(UPLOAD_DIR)

    PRELOADED_DIR = os.path.join(os.path.dirname(__file__), 'preloaded')
    PRELOADED_DIR = os.path.abspath(PRELOADED_DIR)

    RECOMPUTE_FIELD_PROPERTIES = True
    RECOMPUTE_VIZ_SPECS = False

    CELERY_BROKER_URL = 'amqp://admin:password@localhost/dive'
    CELERY_RESULT_BACKEND = 'amqp://'
    # CELERY_RESULT_BACKEND = 'db+postgresql://localhost:5432/dive'

    ALEMBIC_DIR = os.path.join(os.path.dirname(__file__), 'migrate')
    ALEMBIC_DIR = os.path.abspath(ALEMBIC_DIR)

    CELERY_IMPORTS = [
        'dive.tasks.ingestion.upload',
        'dive.tasks.ingestion.dataset_properties',
        'dive.tasks.ingestion.field_properties',
        'dive.tasks.visualization.specs',
        'dive.tasks.statistics.statistics'
    ]

class DevelopmentConfig(BaseConfig):
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://localhost:5432/dive'

class ProductionConfig(BaseConfig):
    SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://localhost:5432/dive'

class TestingConfig(BaseConfig):
    TESTING = True
