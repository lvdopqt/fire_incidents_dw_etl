# Use a multi-architecture Python base image that supports ARM64
FROM python:3.10-slim

# Install git and other necessary packages (like build essentials if needed by pip packages)
# Update package list and install git
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

# Define the working directory inside the container
WORKDIR /usr/app

# Copy the requirements.txt file and install dependencies
# This installs ARM64 versions of libraries if available
COPY requirements.txt ./
RUN pip install --no-cache-dir --no-warn-script-location -r requirements.txt

# Copy the rest of the project code
COPY . .

# Set the default command to bash for interactive use
CMD ["bash"]
ENTRYPOINT []
