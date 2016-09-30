celery worker \
  --app=dive.worker.core \
  --autoscale=10,3 \
  --autoreload \
  -l debug \
  --without-gossip \
  --without-mingle \
  --without-heartbeat
