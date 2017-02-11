#!/bin/bash

echo "*** STARTING POSTGRES ***"
sudo -u postgres psql -U postgres -c "CREATE USER admin WITH PASSWORD 'password';"
sudo -u postgres createdb dive -O admin

echo "*** STARTING RABBITMQ  ***"
sudo rabbitmqctl add_user admin password
sudo rabbitmqctl add_vhost dive
sudo rabbitmqctl set_permissions -p dive admin ".*" ".*" ".*"

echo "*** MIGRATING DB  ***"
python manager.py db init
python manager.py db migrate
python manager.py db upgrade
