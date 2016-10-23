from __future__ import with_statement
from fabric.api import local, env, settings, abort, run, cd
from fabric.contrib.console import confirm
import time

env.use_ssh_config = True
env.hosts = ['dive-server-large', 'dive-server-micro-1', 'dive-server-micro-2']
env.user = 'ubuntu'

code_dir='/var/www/deploy-stage'
app_dir='/var/www/application'
repo='git@github.com:Servers-for-Hackers/deploy-ex.git'
timestamp="release_%s" % int(time.time() * 1000)

def deploy():
    fetch_repo()
    run_composer()
    update_permissions()
    update_symlinks()

def fetch_repo():
    with cd(code_dir):
        with settings(warn_only=True):
            run("mkdir releases")
    with cd("%s/releases" % code_dir):
        run("git clone %s %s" % (repo, timestamp))

def run_composer():
    with cd("%s/releases/%s" % (code_dir, timestamp)):
        run("composer install --prefer-dist")

def update_permissions():
    with cd("%s/releases/%s" % (code_dir, timestamp)):
        run("chgrp -R www-data .")
        run("chmod -R ug+rwx .")

def update_symlinks():
    with cd(code_dir):
        run("ln -nfs %s %s" % (code_dir+'/releases/'+timestamp, app_dir))
        run("chgrp -h www-data %s" % app_dir)
