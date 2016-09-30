FROM continuumio/miniconda

# Install system-level dependencies
# Install dependencies
ADD conda_requirements.txt /
RUN conda create --name dive --file conda_requirements.txt -c anaconda -c asmeurer -c binstar -c bioconda -c birdhouse -c conda-forge -c menpo -c NSIDC -c orchardmile
RUN pip install messytables

# Get into directory
ADD . /DIVE-backend
WORKDIR /DIVE-backend

# Expose port
EXPOSE 5555

# Run server
ENV PATH /opt/conda/envs/dive/bin:$PATH
RUN . ./run_worker.sh
