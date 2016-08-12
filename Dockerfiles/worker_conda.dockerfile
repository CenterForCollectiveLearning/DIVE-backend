FROM continuumio/miniconda

# Install system-level dependencies
# Install dependencies
ADD conda_requirements.txt /
RUN conda create --name dive --file conda_requirements.txt -c anaconda -c asmeurer -c bioconda -c conda-forge -c menpo -c NSIDC -c davidbgonzalez -c prometeia -c elevatedirect -c wakari -c orchardmile

# Get into directory
ADD . /DIVE-backend
WORKDIR /DIVE-backend

# Expose port
EXPOSE 5555

# Run server
ENV PATH /opt/conda/envs/dive/bin:$PATH
RUN . ./run_worker.sh
