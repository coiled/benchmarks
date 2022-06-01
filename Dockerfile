FROM mambaorg/micromamba:0.23.3
COPY --chown=$MAMBA_USER:$MAMBA_USER environments/coiled-runtime-0-0-3-py39.yml /tmp/env.yaml
RUN micromamba install -y -n base -f /tmp/env.yaml && \
    micromamba clean --all --yes