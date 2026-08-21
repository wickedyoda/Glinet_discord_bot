# Use official Python image
FROM python:3.11-slim

# Set working directory inside container
WORKDIR /app

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

# Pre-create runtime directories with restrictive defaults.
RUN mkdir -p /app/data /logs && chmod 700 /app/data /logs

# Install dependencies
COPY requirements.txt .
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
COPY . .

EXPOSE 8080 8081

HEALTHCHECK --interval=30s --timeout=10s --start-period=45s --retries=3 \
  CMD python -c "import os, sys, urllib.request; port = int(os.getenv('WEB_PORT', '8080') or '8080'); url = f'http://127.0.0.1:{port}/readyz'; response = urllib.request.urlopen(url, timeout=8); sys.exit(0 if response.status == 200 else 1)"

# Run the bot
CMD ["python", "-u", "bot.py"]
