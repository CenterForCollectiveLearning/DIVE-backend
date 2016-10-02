celery worker \
  --app=dive.worker.core \
  --autoscale=10,3 \
  --hostname worker1.%h
  --loglevel INFO \
  --without-gossip \
  --without-mingle \
  --without-heartbeat
