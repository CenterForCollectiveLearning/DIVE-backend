#!/bin/sh
DEPENDENCIES="python-dev libffi-dev liblapack-dev gfortran"
apt-get update
apt-get install -y $DEPENDENCIES
