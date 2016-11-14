from celery import Celery
from celery.utils.log import get_task_logger
from dive.base.core import create_app

task_app = create_app()
task_app.app_context().push()
celery = Celery()
celery.config_from_object(task_app.config, namespace='CELERY')
