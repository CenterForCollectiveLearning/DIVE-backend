import os
from celery import Celery
from celery.utils.log import get_task_logger
from dive.base.core import create_app

task_app = create_app()
task_app.app_context().push()
# print task_app.config
os.environ.setdefault('FLASK_SETTINGS_MODULE', 'config.py')
celery = Celery(task_app.import_name)  #, broker=task_app.config['CELERY_BROKER_URL'])
# celery.config_from_object('')
mode = os.environ.get('MODE', 'DEVELOPMENT')
# celery.conf.update(task_app.config)
if mode == 'DEVELOPMENT':
    celery.config_from_object('config.DevelopmentConfig', namespace='CELERY')
elif mode == 'TESTING':
    celery.config_from_object('config.TestingConfig', namespace='CELERY')
elif mode == 'PRODUCTION':
    celery.config_from_object('config.ProductionConfig', namespace='CELERY')
# print celery
celery.autodiscover_tasks()
