from celery import Celery
from celery.utils.log import get_task_logger
from dive.base.core import create_app

task_app = create_app()
task_app.app_context().push()
celery = Celery(task_app.import_name, broker=task_app.config['CELERY_BROKER_URL'])
celery.conf.update(task_app.config)
