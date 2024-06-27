FROM python:3.10-slim-buster


RUN apt-get update
RUN apt-get install -y htop git wget vim

RUN apt-get install -y python3-dev
RUN  apt-get install -y build-essential libssl-dev libatlas-base-dev  libffi-dev libgfortran5 pkg-config cmake gcc

# Install GLPK
RUN apt-get update && apt-get install -y \
    glpk-utils \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Make sure we use the virtualenv:
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN /opt/venv/bin/python -m pip install --upgrade pip setuptools wheel

RUN pip3 install --upgrade pip

RUN pip install  pandas
RUN pip install jupyterlab  matplotlib pyomo 
RUN pip install -U scikit-learn statsmodels scipy statsmodels sympy
RUN pip install refinitiv-data
# # RUN pip install fbprophet
# RUN pip install darts
RUN pip install sktime
RUN pip install shap requests
EXPOSE 8087
RUN mkdir  usr/code
# ENV PATH="/opt/venv/bin:$PATH"
# ENV LD_LIBRARY_PATH="/opt/venv/lib"
# WORKDIR usr/code

ENV JUPYTER_ENABLE_LAB=yes
ENV JUPYTER_TOKEN=docker
CMD ["jupyter-lab", "--ip='*'", "--port=8087", "--no-browser", "--allow-root"]
