DIVE Backend
=================================================
The Data Integration and Visualization Engine (DIVE) is a platform for semi-automatically generating web-based, interactive visualizations of structured data sets. Data visualization is a useful method for understanding complex phenomena, communicating information, and informing inquiry. However, available tools for data visualization are difficult to learn and use, require a priori knowledge of what visualizations to create. See [dive.media.mit.edu](http://dive.media.mit.edu) for more information.

Development setup involves the following steps:
--------
1. Installing system dependencies
2. Setting up postgres
3. Setting up rabbitMQ
4. Starting and entering virtual environment
5. Installing python dependencies
6. Migrating database
7. Starting celery worker
8. Starting server


Install System Dependencies (Linux / apt)
---------
```bash
$ sudo apt-get install -y postgres git python2.7 python-pip build-essential python-dev python-dev libffi-dev liblapack-dev gfortran rabbitmq-server
```

Install System Dependencies (Mac / brew)
---------
Install [Homebrew](http://brew.sh/) if you don't already have it. Then, run the following code:
```
brew install postgres
brew install libmagic
brew install Caskroom/cask/xquartz
brew install cairo
```
Install postgres.app
---------
Install postgres.app by following the instructions here: (http://postgresapp.com/).

Download and open the app to start postgres.

Setup postgres
---------
Make sure that you have a postgres server instance running. Create the dive database by running:
```
createdb dive
```

Start RabbitMQ
---------
1. Run the server as a background process
`sudo rabbitmq-server -detached`

2. Create a RabbitMQ user and virtual host:
```
sudo rabbitmqctl add_user admin password
sudo rabbitmqctl add_vhost dive
sudo rabbitmqctl set_permissions -p dive admin ".*" ".*" ".*"
```


Install and get into a virtual environment
---------
1. Installation: See [this fine tutorial](http://simononsoftware.com/virtualenv-tutorial/).
2. Starting virtual env: `source venv/bin/activate`.


Install Python Dependencies
---------
Within a virtual environment, install dependencies in `requirements.txt`. But due to a dependency issue in numexpr, we need to install numpy first.
```
pip install numpy
pip install -r requirements.txt
```

Start Celery worker
---------
1. Start celery worker: `celery -A dive.task_core worker -l info --autoreload`
2. Start celery monitor (flower): `celery -A dive.task_core flower`


Database Migrations
--------
Follow [the docs](https://flask-migrate.readthedocs.org/en/latest/). The first time, run the migration script.
```bash
python migrate.py db init
```

Then, review and edit the migration script. Finally, each time models ar echanged, run the following:
```
python migrate.py db migrate
python migrate.py db upgrade
```

Run API
---------
1. To run development Flask server, run `python run.py`.
2. To run production Gunicorn server, run `./run.sh`.
