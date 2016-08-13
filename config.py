import os
from os import walk
from os.path import join, dirname, abspath
from dive.base.serialization import dive_json_dumps, dive_json_loads
from kombu.serialization import register
env = os.environ.get
base_dir_path = lambda x: abspath(join(dirname(__file__), x))

register('divejson', dive_json_dumps, dive_json_loads,
    content_type='application/x-myjson',
    content_encoding='utf-8')

class BaseConfig(object):
    # General
    SITE_TITLE = 'dive'
    SECRET_KEY = 'dive'
    PREFERRED_URL_SCHEME = 'http'

    # Flask
    HOST = '0.0.0.0'
    DEBUG = True
    PORT = 8081
    COMPRESS = True
    PROPAGATE_EXCEPTIONS = True

    COOKIE_DOMAIN = None
    REMEMBER_COOKIE_DOMAIN = COOKIE_DOMAIN
    SESSION_COOKIE_DOMAIN = COOKIE_DOMAIN

    # Resources
    STORAGE_TYPE = 'file'
    STORAGE_PATH = base_dir_path('uploads')
    PRELOADED_PATH = base_dir_path('preloaded')
    ALEMBIC_DIR = base_dir_path('migrate')

    # DB
    SQLALCHEMY_POOL_SIZE=20
    SQLALCHEMY_MAX_OVERFLOW=100
    SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://admin:password@localhost/dive'

    # Worker
    CELERY_ALWAYS_EAGER = False
    # CELERY_ACCEPT_CONTENT = ['divejson']
    # CELERY_TASK_SERIALIZER = 'divejson'
    # CELERY_RESULT_SERIALIZER = 'divejson'
    CELERY_BROKER_URL = 'librabbitmq://admin:password@localhost/dive'
    CELERY_RESULT_BACKEND = 'db+postgresql://admin:password@localhost/dive'  # 'amqp'
    CELERY_IMPORTS = []
    for root, dirs, files in walk("./dive/worker"):
        path = root.split('/')
        dir_path = '.'.join(path[1:])
        for f in files:
            if f.endswith('.py') and f != '__init__.py':
                CELERY_IMPORTS.append('%s.%s' % (dir_path, f[:-3]))

    RECOMPUTE_FIELD_PROPERTIES = True
    RECOMPUTE_VIZ_SPECS = True
    RECOMPUTE_STATISTICS = True


class DevelopmentConfig(BaseConfig):
    DEBUG = True

class ProductionConfig(BaseConfig):
    # General
    SITE_TITLE = env('DIVE_SITE_TITLE', 'dive')
    SECRET_KEY = env('DIVE_SECRET', 'dive_secret')
    PREFERRED_URL_SCHEME = env('DIVE_PREFERRED_URL_SCHEME', 'http')

    # Flask
    DEBUG = False
    COMPRESS = False
    # COOKIE_DOMAIN = env('DIVE_COOKIE_DOMAIN', '.usedive.com')
    # REMEMBER_COOKIE_DOMAIN = COOKIE_DOMAIN
    # SESSION_COOKIE_DOMAIN = COOKIE_DOMAIN

    # Resources
    STORAGE_TYPE = env('DIVE_STORAGE_TYPE', 's3')
    if STORAGE_TYPE == 'file':
        STORAGE_PATH = env('DIVE_STORAGE_PATH', '/usr/local/lib/dive')
    else:
        AWS_KEY_ID = env('DIVE_AWS_KEY_ID')
        AWS_SECRET = env('DIVE_AWS_SECRET')
        AWS_DATA_BUCKET = env('DIVE_AWS_DATA_BUCKET')

    DATABASE_URI = '%s:%s@%s/%s' % (env('SQLALCHEMY_DATABASE_USER'), env('SQLALCHEMY_DATABASE_PASSWORD'), env('SQLALCHEMY_DATABASE_ENDPOINT'), env('SQLALCHEMY_DATABASE_NAME'))
    SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://%s' % DATABASE_URI
    CELERY_RESULT_BACKEND = 'db+postgresql://%s' % DATABASE_URI

    # Analytics
    SENTRY_DSN = env('SENTRY_DSN')
    SENTRY_USER_ATTRS = [ 'username', 'email' ]

    # Worker
    CELERY_BROKER_URL = env('DIVE_AMQP_URL')
    CELERY_RESULT_BACKEND = env('DIVE_RESULT_BACKEND')
    RECOMPUTE_FIELD_PROPERTIES = False
    RECOMPUTE_VIZ_SPECS = False
    RECOMPUTE_STATISTICS = False


class TestingConfig(BaseConfig):
    DEBUG = True
