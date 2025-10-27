FROM continuumio/miniconda3:25.3.1-1

# Set working directory
WORKDIR /app

# Update conda
RUN conda update -n base -c defaults conda -y

# Create conda environment with Python 3.9
RUN conda create -n nixtla python=3.9 -y

# Make RUN commands use the new environment
SHELL ["conda", "run", "-n", "nixtla", "/bin/bash", "-c"]

# Install system dependencies in one layer
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install core scientific packages via conda (in one layer for better caching)
RUN conda install -c conda-forge \
    numpy \
    pandas \
    matplotlib \
    seaborn \
    scikit-learn \
    jupyter \
    jupyterlab \
    -y \
    && conda clean -afy

# Install PyTorch packages
RUN conda install pytorch-forecasting pytorch>=1.7 -c pytorch -c conda-forge -y \
    && conda clean -afy

RUN conda install -c conda-forge neuralforecast -y \
# Install Nixtla packages via pip
RUN pip install --no-cache-dir \
    hierarchicalforecast \
    statsforecast \
    mlforecast \
    neuralforecast

# Install additional time series packages
RUN pip install --no-cache-dir \
    plotly \
    lightgbm \
    optuna \
    click \
    openpyxl \
    rich \
    "ray[tune]"

# Create non-root user for security
RUN useradd -m -u 1000 nixtla && \
    chown -R nixtla:nixtla /app

USER nixtla

# Expose JupyterLab port if needed
EXPOSE 8888

# Set default command
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]