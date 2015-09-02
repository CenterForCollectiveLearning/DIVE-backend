#!/bin/bash
pip install virtualenv
virtualenv venv
source venv/bin/activate

# Install numpy first due to numexpr dependencies bug
pip install numpy
pip install -r requirements.txt