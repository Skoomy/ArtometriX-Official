FROM python:3.9-slim-buster


RUN apt-get update
RUN apt-get install -y htop git wget vim

RUN apt-get install -y python3-dev
RUN  apt-get install -y build-essential libssl-dev libatlas-base-dev  libffi-dev libgfortran5 pkg-config cmake gcc



# Make sure we use the virtualenv:
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN /opt/venv/bin/python -m pip install --upgrade pip setuptools wheel

RUN pip install  pandas
RUN pip install jupyterlab  matplotlib
RUN pip install -U scikit-learn statsmodels
# # RUN pip install fbprophet
# RUN pip install darts
# RUN pip install sktime
# RUN pip install shap
EXPOSE 8087
RUN mkdir  usr/code
ENV PATH="/opt/venv/bin:$PATH"
ENV LD_LIBRARY_PATH="/opt/venv/lib"
WORKDIR usr/code