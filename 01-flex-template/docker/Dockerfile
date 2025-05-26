# set base image
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

# Install apache-beam and other dependencies to launch the pipeline
# Atualiza pacotes do sistema e o pip
RUN apt-get update && apt-get install -y --no-install-recommends \
    && pip install --no-cache-dir --upgrade pip

# Instala o Cython antes do Apache Beam
RUN pip install --no-cache-dir Cython

# Remove Apache Beam antigo da imagem base (caso esteja presente)
RUN pip uninstall -y apache-beam || true

# Instala explicitamente os pacotes desejados
RUN pip install --no-cache-dir apache-beam[gcp]==2.65.0 requests==2.31.0

# Since we already downloaded all the dependencies, there's no need to rebuild everything.
ENV PIP_NO_DEPS=True
