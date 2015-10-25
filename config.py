import os

class BaseConfig(object):
    DEBUG = False
    PORT = 8081

    FIELD_RELATIONSHIP_DISTANCE_THRESHOLD = 0.8

    UPLOAD_DIR = os.path.join(os.path.dirname(__file__), 'uploads')
    UPLOAD_DIR = os.path.abspath(UPLOAD_DIR)

    PRELOADED_DIR = os.path.join(os.path.dirname(__file__), 'preloaded')
    PRELOADED_DIR = os.path.abspath(PRELOADED_DIR)

    RECOMPUTE_FIELD_PROPERTIES = True
    RECOMPUTE_VIZ_SPECS = True
    RECOMPUTE_STATISTICS = False

    CELERY_BROKER_URL = 'amqp://admin:password@localhost/dive'
    CELERY_RESULT_BACKEND = 'amqp://'

    SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://admin:password@localhost:5432/dive'

    ALEMBIC_DIR = os.path.join(os.path.dirname(__file__), 'migrate')
    ALEMBIC_DIR = os.path.abspath(ALEMBIC_DIR)

    CELERY_IMPORTS = [
        'dive.tasks.ingestion.upload',
        'dive.tasks.ingestion.dataset_properties',
        'dive.tasks.ingestion.type_detection',
        'dive.tasks.ingestion.type_classes',
        'dive.tasks.ingestion.field_properties',
        'dive.tasks.ingestion.relationships',
        'dive.tasks.visualization.specs',
        'dive.tasks.statistics.regression',
        'dive.tasks.statistics.comparison',
        'dive.tasks.statistics.segmentation',
    ]

class DevelopmentConfig(BaseConfig):
    DEBUG = True

class ProductionConfig(BaseConfig):
    DEBUG = True

class TestingConfig(BaseConfig):
    TESTING = True
