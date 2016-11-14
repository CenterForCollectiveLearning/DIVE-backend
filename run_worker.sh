celery worker \
  -E \
  --app=dive.worker.core \
  --autoscale=10,3 \
  --loglevel=DEBUG \
  # --without-gossip \
  # --without-mingle \
  # --without-heartbeat
