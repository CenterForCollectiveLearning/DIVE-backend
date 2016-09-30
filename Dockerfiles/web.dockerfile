FROM continuumio/miniconda

# Install system-level dependencies
# Install dependencies
ADD conda_requirements.txt /
RUN conda install --file conda_requirements.txt -c anaconda -c asmeurer -c bioconda -c conda-forge -c menpo -c NSIDC -c davidbgonzalez -c prometeia -c wakari -c orchardmile

# Get into directory
ADD . /DIVE-backend
WORKDIR /DIVE-backend

# Expose API PORT
EXPOSE 8081

# Run server
# CMD ["python", "run_server.py"]
RUN . ./run_server.sh
