celery worker \
  --app=dive.worker.core \
  -l debug \
  --without-gossip \
  --without-mingle \
  --without-heartbeat
