from __future__ import with_statement
from fabric.api import local, env, settings, abort, run, cd, hosts
from fabric.contrib.console import confirm
import time

env.use_ssh_config = True
api_hosts = ['dive-api-aws']
worker_hosts = ['dive-worker-aws']

code_dir='~/DIVE-backend'


@hosts(api_hosts)
def deploy_api():
    fetch_repo()
    run("tmux a -t dive")
    run("pkill gunicorn")
    run("source venv/bin/activate")
    run("sh run_server.sh")

@hosts(worker_hosts)
def deploy_worker():
    fetch_repo()


def fetch_repo():
    with cd(code_dir):
        with settings(warn_only=True):
            run("source venv/bin/activate")
            # run("pip install -r requirements.txt")
            run("git pull")
