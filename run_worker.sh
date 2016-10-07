celery worker \
  --app=dive.worker.core \
  --autoscale=10,3 \
  --loglevel=INFO \
  --without-gossip \
  --without-mingle \
  --without-heartbeat
