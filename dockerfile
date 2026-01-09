# Use a base image from Jupyter Docker Stacks
FROM jupyter/base-notebook

# Copy your requirements file into the container
COPY ./requirements.txt /home/jovyan/work/requirements.txt

# Install the packages
RUN pip install --no-cache-dir -r /home/jovyan/work/requirements.txt
RUN pip install --upgrade  "pyarrow>=8.0.0"

# Set the working directory to the mounted volume directory
WORKDIR /home/jovyan/work