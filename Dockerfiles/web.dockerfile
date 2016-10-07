FROM python:2.7

# Install system-level dependencies
# Install dependencies
ADD requirements.txt /
RUN pip install -r requirements.txt

# Get into directory
ADD . /DIVE-backend
WORKDIR /DIVE-backend

# Expose API PORT
EXPOSE 8081

# Run server
RUN . ./run_server.sh
