import os
from os import walk
from os.path import join, dirname, abspath
import sys
from dive.base.serialization import pjson_dumps, pjson_loads
from kombu.serialization import register, registry
env = os.environ.get
base_dir_path = lambda x: abspath(join(dirname(__file__), x))


# Register custom PJSON to celery
register('pjson', pjson_dumps, pjson_loads,
    content_type='application/x-pjson',
    content_encoding='utf-8')

registry.enable('application/x-pjson')


class BaseConfig(object):
    # General
    SITE_URL = 'localhost:3009'
    SITE_TITLE = 'dive'
    SECRET_KEY = 'dive'
    PREFERRED_URL_SCHEME = 'http'
    SECURITY_PASSWORD_SALT = 'nacl'

    # Flask
    HOST = '0.0.0.0'
    DEBUG = True
    PORT = 8081
    COMPRESS = True
    PROPAGATE_EXCEPTIONS = True

    # Cookies
    COOKIE_DOMAIN = None
    REMEMBER_COOKIE_DOMAIN = COOKIE_DOMAIN
    SESSION_COOKIE_DOMAIN = COOKIE_DOMAIN

    # Mail
    MAIL_AUTHENTICATION = True
    MAIL_SERVER = 'smtp.postmarkapp.com'
    MAIL_USERNAME = '99b4b664-9751-492c-b48d-2bd492e9912a'
    MAIL_PASSWORD = '99b4b664-9751-492c-b48d-2bd492e9912a'
    MAIL_PORT = 2525
    MAIL_USE_TLS = True
    MAIL_USE_SSL = False
    MAIL_DEFAULT_SENDER = 'dive@media.mit.edu'
    MAIL_SUPPRESS_SEND = False
    MAIL_DEBUG = False

    # Data
    MAX_CONTENT_LENGTH = sys.maxint
    ROW_LIMIT = sys.maxint
    COLUMN_LIMIT = sys.maxint

    # Parameters
    ANALYSIS_DATA_SIZE_CUTOFF=10000
    ANALYSIS_CATEGORICAL_VALUE_LIMIT=20

    # Resources
    METADATA_FILE_NAME_SUFFIX = 'dev'
    STORAGE_TYPE = 'file'
    STORAGE_PATH = env('DIVE_STORAGE_PATH', base_dir_path('uploads'))
    AWS_ACCESS_KEY_ID = env('DIVE_AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = env('DIVE_AWS_SECRET_ACCESS_KEY')
    AWS_DATA_BUCKET = env('DIVE_AWS_DATA_BUCKET')
    AWS_REGION = env('DIVE_AWS_REGION')
    PRELOADED_PATH = base_dir_path('preloaded')

    # DB
    DATABASE_URI = 'admin:password@localhost/dive'
    SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://%s?client_encoding=utf8' % DATABASE_URI
    SQLALCHEMY_POOL_SIZE=20
    SQLALCHEMY_MAX_OVERFLOW=100
    # SQLALCHEMY_ECHO='debug'
    ALEMBIC_DIR = base_dir_path('migrate')

    # Worker
    CELERY_TASK_ALWAYS_EAGER = False
    CELERY_ACCEPT_CONTENT = [ 'pjson' ]
    CELERY_TASK_SERIALIZER = 'pjson'
    CELERY_RESULT_SERIALIZER = 'pjson'
    CELERY_BROKER_URL = 'amqp://admin:password@localhost/dive'
    CELERY_RESULT_BACKEND = 'db+postgresql://%s' % DATABASE_URI

    CELERY_IMPORTS = []
    for root, dirs, files in walk("./dive/worker"):
        path = root.split('/')
        dir_path = '.'.join(path[1:])
        for f in files:
            if f.endswith('.py') and f != '__init__.py':
                CELERY_IMPORTS.append('%s.%s' % (dir_path, f[:-3]))

    # Result persistence
    RECOMPUTE_FIELD_PROPERTIES = True
    RECOMPUTE_VIZ_SPECS = True
    RECOMPUTE_STATISTICS = True


class DevelopmentConfig(BaseConfig):
    DEBUG = True

    # Mail
    MAIL_AUTHENTICATION = False

class ProductionConfig(BaseConfig):
    # General
    SITE_URL = 'staging.usedive.com'
    SITE_TITLE = env('DIVE_SITE_TITLE', 'dive')
    SECRET_KEY = env('DIVE_SECRET', 'dive_secret')
    PREFERRED_URL_SCHEME = env('DIVE_PREFERRED_URL_SCHEME', 'https')

    # Flask
    DEBUG = False
    COMPRESS = False
    COOKIE_DOMAIN = env('DIVE_COOKIE_DOMAIN', 'usedive.com')
    REMEMBER_COOKIE_DOMAIN = COOKIE_DOMAIN
    SESSION_COOKIE_DOMAIN = COOKIE_DOMAIN

    # Analytics
    SENTRY_DSN = env('SENTRY_DSN')
    SENTRY_USER_ATTRS = [ 'username', 'email' ]

    # Mail
    MAIL_AUTHENTICATION = True    
    MAIL_USERNAME = env('DIVE_MAIL_USERNAME')
    MAIL_PASSWORD = env('DIVE_MAIL_PASSWORD')

    # Data
    MAX_CONTENT_LENGTH = 10 * 1024 * 1024
    ROW_LIMIT = 100000
    COLUMN_LIMIT = 60

    # Resources
    METADATA_FILE_NAME_SUFFIX = 'PROD'
    STORAGE_TYPE = env('DIVE_STORAGE_TYPE', 'file')
    if STORAGE_TYPE == 'file':
        STORAGE_PATH = env('DIVE_STORAGE_PATH', base_dir_path('uploads'))
    else:
        AWS_ACCESS_KEY_ID = env('DIVE_AWS_ACCESS_KEY_ID')
        AWS_SECRET_ACCESS_KEY = env('DIVE_AWS_SECRET_ACCESS_KEY')
        AWS_DATA_BUCKET = env('DIVE_AWS_DATA_BUCKET')
        AWS_REGION = env('DIVE_AWS_REGION')

    # DB
    DATABASE_URI = '%s:%s@%s/%s' % (env('SQLALCHEMY_DATABASE_USER'), env('SQLALCHEMY_DATABASE_PASSWORD'), env('SQLALCHEMY_DATABASE_ENDPOINT'), env('SQLALCHEMY_DATABASE_NAME'))
    SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://%s?client_encoding=utf8' % DATABASE_URI

    # Worker
    CELERY_BROKER_URL = env('DIVE_AMQP_URL', 'librabbitmq://admin:password@localhost/dive')
    CELERY_RESULT_BACKEND =  'db+postgresql://%s' % DATABASE_URI

    # S3
    AWS_ACCESS_KEY_ID = env('DIVE_AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = env('DIVE_AWS_SECRET_ACCESS_KEY')
    AWS_DATA_BUCKET = env('DIVE_AWS_DATA_BUCKET')
    AWS_REGION = env('DIVE_AWS_REGION')

    # Result persistence
    RECOMPUTE_FIELD_PROPERTIES = False
    RECOMPUTE_VIZ_SPECS = False
    RECOMPUTE_STATISTICS = False


class TestingConfig(BaseConfig):
    # General
    SITE_TITLE = env('DIVE_SITE_TITLE', 'dive')
    SECRET_KEY = env('DIVE_SECRET', 'dive_secret')
    PREFERRED_URL_SCHEME = env('DIVE_PREFERRED_URL_SCHEME', 'https')

    # Flask
    DEBUG = False
    COMPRESS = False
    COOKIE_DOMAIN = env('DIVE_COOKIE_DOMAIN', 'usedive.com')
    REMEMBER_COOKIE_DOMAIN = COOKIE_DOMAIN
    SESSION_COOKIE_DOMAIN = COOKIE_DOMAIN

    # Analytics
    SENTRY_DSN = env('SENTRY_DSN')
    SENTRY_USER_ATTRS = [ 'username', 'email' ]

    # Resources
    STORAGE_TYPE = env('DIVE_STORAGE_TYPE', 'file')
    if STORAGE_TYPE == 'file':
        STORAGE_PATH = env('DIVE_STORAGE_PATH', base_dir_path('uploads'))
    else:
        AWS_ACCESS_KEY_ID = env('DIVE_AWS_ACCESS_KEY_ID')
        AWS_SECRET_ACCESS_KEY = env('DIVE_AWS_SECRET_ACCESS_KEY')
        AWS_DATA_BUCKET = env('DIVE_AWS_DATA_BUCKET')
        AWS_REGION = env('DIVE_AWS_REGION')

    # DB
    DATABASE_URI = '%s:%s@%s/%s' % (env('SQLALCHEMY_DATABASE_USER'), env('SQLALCHEMY_DATABASE_PASSWORD'), env('SQLALCHEMY_DATABASE_ENDPOINT'), env('SQLALCHEMY_DATABASE_NAME'))
    SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://%s?client_encoding=utf8' % DATABASE_URI

    # Worker
    CELERY_BROKER_URL = env('DIVE_AMQP_URL', 'librabbitmq://admin:password@localhost/dive')
    CELERY_RESULT_BACKEND =  'db+postgresql://%s' % DATABASE_URI

    # S3
    AWS_ACCESS_KEY_ID = env('DIVE_AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = env('DIVE_AWS_SECRET_ACCESS_KEY')
    AWS_DATA_BUCKET = env('DIVE_AWS_DATA_BUCKET')
    AWS_REGION = env('DIVE_AWS_REGION')

    # Result persistence
    RECOMPUTE_FIELD_PROPERTIES = True
    RECOMPUTE_VIZ_SPECS = True
    RECOMPUTE_STATISTICS = True
