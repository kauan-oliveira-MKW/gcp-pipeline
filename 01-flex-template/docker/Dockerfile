FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

# copy files
COPY ./requirements.txt ${WORKDIR}/
COPY ./pipeline.py ${WORKDIR}/

# set environment variables
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/pipeline.py"

# Install build dependencies and update pip/setuptools/wheel
RUN apt-get update && apt-get install -y --no-install-recommends build-essential python3-dev gcc cython && apt-get clean
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Install python dependencies
RUN pip install --no-cache-dir -U -r ${WORKDIR}/requirements.txt

# Since we already downloaded all the dependencies, there's no need to rebuild everything.
ENV PIP_NO_DEPS=True
