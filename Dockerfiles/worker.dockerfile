FROM python:2.7

# Install system-level dependencies
# Install dependencies
ADD requirements.txt /
RUN pip install -r requirements.txt

# Get into directory
ADD . /DIVE-backend
WORKDIR /DIVE-backend

# Run server
RUN . ./run_worker.sh
