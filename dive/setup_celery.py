from celery.utils.log import get_task_logger
from dive.core import create_app, create_celery

log = get_task_logger(__name__)

flask_app = create_app()
celery = create_celery(flask_app)
