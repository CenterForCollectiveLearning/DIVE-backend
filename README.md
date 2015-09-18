DIVE Backend
=================================================
The Data Integration and Visualization Engine (DIVE) is a platform for semi-automatically generating web-based, interactive visualizations of structured data sets. Data visualization is a useful method for understanding complex phenomena, communicating information, and informing inquiry. However, available tools for data visualization are difficult to learn and use, require a priori knowledge of what visualizations to create. See [dive.media.mit.edu](http://dive.media.mit.edu) for more information.

Install System Dependencies (Mac / brew)
---------
Make

Install System Dependencies (Linux / apt)
---------
```bash
$ sudo apt-get install -y postgres git python2.7 python-pip build-essential python-dev python-dev libffi-dev liblapack-dev gfortran
$ sudo su postgres
$ createuser -D -P -R -S dive
$ createdb -E utf8 -O dive -T template0 spendb
```

Using Virtual Env to Manage Server-Side Python Dependencies
---------
0. Install [Homebrew](http://brew.sh/) if you don't already have it.
1. Installation: See [this fine tutorial](http://simononsoftware.com/virtualenv-tutorial/).
2. Freezing virtual env packages: `pip freeze > requirements.txt`.
3. Starting virtual env: `source venv/bin/activate`.
4. Reloading from `requirements.txt` (while virtualenv is active): `pip install -r requirements.txt`.
4. Install XQuartz: `brew install Caskroom/cask/xquartz`.
5. Install Cairo: `brew install cairo`.

Run Server-Side Code / API
---------
1. Load virtual environment.
2. Run mongod: `mongod --dbpath server/uploads`
3. In active virtual environment with all dependencies, in base directory, run shell script to activate Gunicorn server: `sh server/run.sh`. Or, run Flask server with `python server/run.py`.

Linux installation (with apt-get available)
---------
1. In base directory, run `./install-apt.sh` to install system-label dependencies
2. Then, run `./install-python-dep.sh`. to
