FROM mambaorg/micromamba:0.23.3
ARG ENV_FILENAME
COPY --chown=$MAMBA_USER:$MAMBA_USER environments/$ENV_FILENAME /tmp/env.yaml
RUN micromamba install -y -n base -f /tmp/env.yaml && \
    micromamba clean --all --yes