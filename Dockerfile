FROM mambaorg/micromamba:0.23.3
ARG ENV_FILENAME
COPY --chown=$MAMBA_USER:$MAMBA_USER environments/$ENV_FILENAME.yml /tmp/env.yml
RUN micromamba install -y -n base -f /tmp/env.yml && \
    micromamba clean --all --yes