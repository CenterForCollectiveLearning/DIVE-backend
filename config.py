import os
from os import walk
from os.path import join, dirname, abspath
from dive.base.serialization import pjson_dumps, pjson_loads
from kombu.serialization import register
env = os.environ.get
base_dir_path = lambda x: abspath(join(dirname(__file__), x))

register('pjson', pjson_dumps, pjson_loads,
    content_type='application/x-pjson',
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
    MAX_CONTENT_LENGTH = 100 * 1024 * 1024

    # Resources
    # STORAGE_TYPE = env('DIVE_STORAGE_TYPE', 'file')
    STORAGE_TYPE = 'file'
    if STORAGE_TYPE == 'file':
        STORAGE_PATH = env('DIVE_STORAGE_PATH', base_dir_path('uploads'))
    else:
        AWS_ACCESS_KEY_ID = env('DIVE_AWS_ACCESS_KEY_ID')
        AWS_SECRET_ACCESS_KEY = env('DIVE_AWS_SECRET_ACCESS_KEY')
        AWS_DATA_BUCKET = env('DIVE_AWS_DATA_BUCKET')
        AWS_REGION = env('DIVE_AWS_REGION')
    PRELOADED_PATH = base_dir_path('preloaded')

    # DB
    DATABASE_URI = 'admin:password@localhost/dive'
    # DATABASE_URI = '%s:%s@%s/%s' % (env('SQLALCHEMY_DATABASE_USER'), env('SQLALCHEMY_DATABASE_PASSWORD'), env('SQLALCHEMY_DATABASE_ENDPOINT'), env('SQLALCHEMY_DATABASE_NAME'))
    SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://%s?client_encoding=utf8' % DATABASE_URI
    SQLALCHEMY_POOL_SIZE=20
    SQLALCHEMY_MAX_OVERFLOW=100
    ALEMBIC_DIR = base_dir_path('migrate')

    # Worker
    CELERY_ALWAYS_EAGER = False
    CELERY_ACCEPT_CONTENT = [ 'pjson' ]
    CELERY_TASK_SERIALIZER = 'pjson'
    CELERY_RESULT_SERIALIZER = 'pjson'
    CELERY_BROKER_URL = 'librabbitmq://admin:password@localhost/dive'
    # CELERY_BROKER_URL = env('DIVE_AMQP_URL', 'librabbitmq://admin:password@localhost/dive')
    CELERY_RESULT_BACKEND =  'db+postgresql://%s' % DATABASE_URI
    CELERY_IMPORTS = []
    for root, dirs, files in walk("./dive/worker"):
        path = root.split('/')
        dir_path = '.'.join(path[1:])
        for f in files:
            if f.endswith('.py') and f != '__init__.py':
                CELERY_IMPORTS.append('%s.%s' % (dir_path, f[:-3]))

    BROKER_POOL_LIMIT = 1 # Will decrease connection usage
    BROKER_HEARTBEAT = None # We're using TCP keep-alive instead
    BROKER_CONNECTION_TIMEOUT = 30 # May require a long timeout due to Linux DNS timeouts etc
    CELERY_SEND_EVENTS = False # Will not create celeryev.* queues
    CELERY_EVENT_QUEUE_EXPIRES = 60

    # Result persistence
    RECOMPUTE_FIELD_PROPERTIES = True
    RECOMPUTE_VIZ_SPECS = True
    RECOMPUTE_STATISTICS = True


class DevelopmentConfig(BaseConfig):
    DEBUG = True

class ProductionConfig(BaseConfig):
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

    # DB
    DATABASE_URI = '%s:%s@%s/%s' % (env('SQLALCHEMY_DATABASE_USER'), env('SQLALCHEMY_DATABASE_PASSWORD'), env('SQLALCHEMY_DATABASE_ENDPOINT'), env('SQLALCHEMY_DATABASE_NAME'))
    SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://%s?client_encoding=utf8' % DATABASE_URI

    # Worker
    CELERY_BROKER_URL = env('DIVE_AMQP_URL', 'librabbitmq://admin:password@localhost/dive')
    CELERY_RESULT_BACKEND =  'db+postgresql://%s' % DATABASE_URI
    BROKER_POOL_LIMIT = 1 # Will decrease connection usage
    BROKER_HEARTBEAT = None # We're using TCP keep-alive instead
    BROKER_CONNECTION_TIMEOUT = 30 # May require a long timeout due to Linux DNS timeouts etc
    CELERY_SEND_EVENTS = False # Will not create celeryev.* queues
    CELERY_EVENT_QUEUE_EXPIRES = 60

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
    PREFERRED_URL_SCHEME = env('DIVE_PREFERRED_URL_SCHEME', 'http')

    # Flask
    DEBUG = True
    COMPRESS = False

    # DB
    DATABASE_URI = '%s:%s@%s/%s' % (env('SQLALCHEMY_DATABASE_USER'), env('SQLALCHEMY_DATABASE_PASSWORD'), env('SQLALCHEMY_DATABASE_ENDPOINT'), env('SQLALCHEMY_DATABASE_NAME'))
    SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://%s' % DATABASE_URI

    # Analytics
    SENTRY_DSN = env('SENTRY_DSN')
    SENTRY_USER_ATTRS = [ 'username', 'email' ]

    # Worker
    CELERY_BROKER_URL = env('DIVE_AMQP_URL', 'librabbitmq://admin:password@localhost/dive')
    CELERY_RESULT_BACKEND =  'db+postgresql://%s' % DATABASE_URI
    BROKER_POOL_LIMIT = 1 # Will decrease connection usage
    BROKER_HEARTBEAT = None # We're using TCP keep-alive instead
    BROKER_CONNECTION_TIMEOUT = 30 # May require a long timeout due to Linux DNS timeouts etc
    CELERY_SEND_EVENTS = False # Will not create celeryev.* queues
    CELERY_EVENT_QUEUE_EXPIRES = 60

    # S3
    AWS_ACCESS_KEY_ID = env('DIVE_AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = env('DIVE_AWS_SECRET_ACCESS_KEY')
    AWS_DATA_BUCKET = env('DIVE_AWS_DATA_BUCKET')
    AWS_REGION = env('DIVE_AWS_REGION')

    # Result persistence
    RECOMPUTE_FIELD_PROPERTIES = False
    RECOMPUTE_VIZ_SPECS = False
    RECOMPUTE_STATISTICS = False
