# Use official Python image for reproducibility
FROM python:3.11-slim

# Refresh base OS packages so security fixes from the Debian slim image
# are applied even when the parent image lags behind the latest point release.
# Upgrades OS packages to latest patched versions (not just installed ones)
# to address Trivy container alerts for libblkid1, perl, openssl, ncurses,
# gzip, libacl, etc.
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    libc-bin \
    libc6 \
    libblkid1 \
    libsmartcols1 \
    libncursesw6 \
    libtinfo6 \
    ncurses-base \
    ncurses-bin \
    openssl \
    libssl3t64 \
    openssl-provider-legacy \
    perl-base \
    perl \
    gzip \
    libacl1 \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Create a non-root user/group for the bot process
RUN groupadd -r bot && useradd -r -g bot bot

# Pre-create runtime directories with restrictive defaults and set as working dir FIRST
RUN mkdir -p /app/data /logs && chmod 700 /app/data /logs && chown -R bot:bot /app /logs
WORKDIR /app
USER bot

# Install dependencies
COPY --chown=bot:bot requirements.txt .
RUN python -m pip install --no-cache-dir --upgrade \
    --root-user-action=ignore \
    pip \
    "setuptools>=78.1.1" \
    "msgpack>=1.2.1" \
    "cryptography>=48.0.1" \
    "wheel>=0.46.2" \
    "jaraco.context>=6.1.0" \
  && python -m pip install --no-cache-dir --root-user-action=ignore -r requirements.txt

# Copy the bot code and env files into the container
COPY --chown=bot:bot . .

EXPOSE 8080 8081

# Healthcheck: when web admin is disabled, always pass; otherwise check the readyz endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=45s --retries=3 \
  CMD python -c "import os,sys,urllib.request; we=os.getenv('WEB_ENABLED','true').strip().lower() not in {'0','false','no','off'}; sys.exit(0) if not we else (lambda r: sys.exit(0 if r.status==200 else 1))(urllib.request.urlopen(f'http://127.0.0.1:{int(os.getenv(\"WEB_PORT\",\"8080\") or \"8080\")}/readyz',timeout=8))"

# Run the bot
CMD ["python", "-u", "bot.py"]
