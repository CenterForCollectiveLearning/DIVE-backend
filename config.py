import os
from os import walk
from os.path import join, dirname, abspath
env = os.environ.get
base_dir_path = lambda x: abspath(join(dirname(__file__), x))

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
    CELERY_ACCEPT_CONTENT = ['json']
    CELERY_TASK_SERIALIZER = 'json'
    CELERY_RESULT_SERIALIZER = 'json'
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
    COOKIE_DOMAIN = env('DIVE_COOKIE_DOMAIN', '.usedive.com')
    REMEMBER_COOKIE_DOMAIN = COOKIE_DOMAIN
    SESSION_COOKIE_DOMAIN = COOKIE_DOMAIN

    STORAGE_TYPE = env('DIVE_STORAGE_TYPE', 's3')
    if STORAGE_TYPE == 'file':
        STORAGE_PATH = env('DIVE_STORAGE_PATH', '/usr/local/lib/dive')
    else:
        AWS_KEY_ID = env('DIVE_AWS_KEY_ID')
        AWS_SECRET = env('DIVE_AWS_SECRET')
        AWS_DATA_BUCKET = env('DIVE_AWS_DATA_BUCKET')

    # Analytics
    SENTRY_DSN = ''
    SENTRY_USER_ATTRS = [ 'username', 'email' ]

    RECOMPUTE_FIELD_PROPERTIES = False
    RECOMPUTE_VIZ_SPECS = False
    RECOMPUTE_STATISTICS = False


class TestingConfig(BaseConfig):
    DEBUG = True
