import os

class BaseConfig(object):
    DEBUG = False
    HOST = '0.0.0.0'
    PORT = 8081

    SECRET_KEY = 'dive'

    SENTRY_DSN = ''
    SENTRY_USER_ATTRS = [ 'username', 'email' ]

    FIELD_RELATIONSHIP_DISTANCE_THRESHOLD = 0.8

    UPLOAD_DIR = os.path.join(os.path.dirname(__file__), 'uploads')
    UPLOAD_DIR = os.path.abspath(UPLOAD_DIR)

    PRELOADED_DIR = os.path.join(os.path.dirname(__file__), 'preloaded')
    PRELOADED_DIR = os.path.abspath(PRELOADED_DIR)

    RECOMPUTE_FIELD_PROPERTIES = True
    RECOMPUTE_VIZ_SPECS = True
    RECOMPUTE_STATISTICS = True

    PROPAGATE_EXCEPTIONS = True

    COMPRESS = True

    SQLALCHEMY_POOL_SIZE=20
    SQLALCHEMY_MAX_OVERFLOW=100
    SQLALCHEMY_TRACK_MODIFICATIONS=False

    COOKIE_DOMAIN = None
    REMEMBER_COOKIE_DOMAIN = COOKIE_DOMAIN
    SESSION_COOKIE_DOMAIN = COOKIE_DOMAIN

    CELERY_BROKER_URL = 'librabbitmq://admin:password@%s/dive' % 'rabbitmq'
    CELERY_RESULT_BACKEND = 'amqp'

    SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://admin:password@%s:5432/dive' % 'localhost'

    ALEMBIC_DIR = os.path.join(os.path.dirname(__file__), 'migrate')
    ALEMBIC_DIR = os.path.abspath(ALEMBIC_DIR)

    CELERY_IMPORTS = [
        'dive.tasks.pipelines',
        'dive.tasks.handlers',
        'dive.tasks.ingestion.upload',
        'dive.tasks.ingestion.dataset_properties',
        'dive.tasks.ingestion.id_detection',
        'dive.tasks.ingestion.type_detection',
        'dive.tasks.ingestion.type_classes',
        'dive.tasks.ingestion.field_properties',
        'dive.tasks.ingestion.relationships',
        'dive.tasks.transformation.reduce',
        'dive.tasks.visualization.__init__',
        'dive.tasks.visualization.data',
        'dive.tasks.visualization.enumerate_specs',
        'dive.tasks.visualization.score_specs',
        'dive.tasks.visualization.spec_pipeline',
        'dive.tasks.visualization.type_mapping',
        'dive.tasks.visualization.marginal_spec_functions.single_field_single_type_specs',
        'dive.tasks.visualization.marginal_spec_functions.single_field_multi_type_specs',
        'dive.tasks.visualization.marginal_spec_functions.multi_field_single_type_specs',
        'dive.tasks.visualization.marginal_spec_functions.mixed_field_multi_type_specs',
        'dive.tasks.visualization.marginal_spec_functions.multi_field_multi_type_specs',
        'dive.tasks.transformation.join',
        'dive.tasks.transformation.pivot',
        'dive.tasks.transformation.reduce',
        'dive.tasks.statistics.regression',
        'dive.tasks.statistics.comparison',
        'dive.tasks.statistics.aggregation',
    ]

class DevelopmentConfig(BaseConfig):
    DEBUG = True

class ProductionConfig(BaseConfig):
    DEBUG = True
    RECOMPUTE_FIELD_PROPERTIES = False
    RECOMPUTE_VIZ_SPECS = False
    RECOMPUTE_STATISTICS = False

    COOKIE_DOMAIN = '.usedive.com'
    REMEMBER_COOKIE_DOMAIN = COOKIE_DOMAIN
    SESSION_COOKIE_DOMAIN = COOKIE_DOMAIN

    COMPRESS = False

class TestingConfig(BaseConfig):
    TESTING = True
