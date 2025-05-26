# Imagem base do Dataflow Flex Template
FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

# Diretório de trabalho
ARG WORKDIR=/dataflow/template
WORKDIR ${WORKDIR}
RUN mkdir -p ${WORKDIR}

# Copia o arquivo principal do pipeline
COPY ./pipeline.py ${WORKDIR}/

# Variáveis de ambiente exigidas pelo Dataflow Flex Template
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=""
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/pipeline.py"

# Atualiza pacotes do sistema e o pip
RUN apt-get update && apt-get install -y --no-install-recommends \
    && pip install --no-cache-dir --upgrade pip

# Remove Apache Beam antigo da imagem base (caso esteja presente)
RUN pip uninstall -y apache-beam || true

RUN find / -type d -name "apache_beam*" -exec rm -rf {} +

# Instala explicitamente os pacotes desejados
RUN pip install apache-beam[gcp]==2.65.0 requests==2.31.0

RUN python -c "import apache_beam as beam; print('Apache Beam version:', beam.__version__)"

# Evita reinstalação de dependências no runtime
ENV PIP_NO_DEPS=True
