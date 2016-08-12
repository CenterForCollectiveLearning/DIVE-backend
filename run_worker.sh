celery -A dive.task_core worker -l info --autoscale=10,3 --autoreload &
