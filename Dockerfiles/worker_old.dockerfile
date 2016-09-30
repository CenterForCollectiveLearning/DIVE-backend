FROM python:2.7

# Install system-level dependencies
RUN apt-get update && apt-get install -y \
  build-essential \
  python2.7 \
  python-pip \
  python-dev \
  libpq-dev \
  libffi-dev \
  libatlas-dev \
  liblapack-dev \
  gfortran \
  python-numpy \
  python-scipy \
  python-pandas \
  python-lxml \
  python-sklearn \
  lib32z1-dev \
  libxml2-dev \
  libxslt1-dev \
  lib32ncurses5-dev

# Install dependencies
ADD requirements.txt /
RUN pip install -r requirements.txt

# Get into directory
ADD . /DIVE-backend
WORKDIR /DIVE-backend

# Expose port
EXPOSE 5555

# Run server
CMD "./run_celery.sh"
