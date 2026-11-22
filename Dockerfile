# Use official Python image for reproducibility
# Pinned by digest: a tag can be repointed at a different image without warning.
FROM python:3.15-slim@sha256:65a65fc7b30b06cb6df4930fd4650697cb23f4c2698ab81fc87e4b031bc36928

# Refresh base OS packages so security fixes from the Debian slim image are applied
# even when the parent image lags behind the latest point release. The explicit list
# pulls the patched build of each package the scanner reported, rather than trusting
# whatever the parent image shipped.
#
# `perl` is deliberately absent: the full interpreter is not used by this image, and it
# pulled in perl-modules-5.40 and libperl5.40, three packages carrying a HIGH advisory
# with no Debian fix. `perl-base` stays -- dpkg and apt need it -- and the bot, the
# healthcheck and the test suite all run without the full package.
RUN apt-get update \
  && apt-get upgrade -y --no-install-recommends \
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
    gzip \
    libacl1 \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Create a non-root user/group for the bot process
RUN groupadd -r bot && useradd -r -g bot bot

# Pre-create runtime directories with restrictive defaults
RUN mkdir -p /app/data /logs && chmod 700 /app/data /logs && chown -R bot:bot /app /logs

# Install dependencies BEFORE switching to non-root user
# so pip can write to global site-packages
WORKDIR /app
COPY --chown=bot:bot requirements.txt .
# pip is pinned exactly like every other dependency; the build tools that used to be
# listed here are in requirements.txt already, and pinning them twice invites drift.
RUN python -m pip install --no-cache-dir "pip==26.3" \
  && python -m pip install --no-cache-dir -r requirements.txt \
  # pip ships a CycloneDX BOM describing the versions its vendored code was built
  # against -- including setuptools 70.3.0, which is not installed here. Image
  # scanners read it as an installed package and report three setuptools CVEs
  # against a version that does not exist in the image; the resolved setuptools is
  # 84.2.1, above every one of those fixes. Nothing reads this file at runtime.
  && rm -f /usr/local/lib/python*/site-packages/pip/_vendor/bom.cdx.json

# Copy the bot code and env files into the container
COPY --chown=bot:bot . .

# Switch to non-root user AFTER pip install
USER bot

EXPOSE 8082 8083

# Healthcheck: when web admin is disabled, always pass; otherwise check the readyz endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=45s --retries=3 \
  CMD python /app/healthcheck.py

# Run the bot
CMD ["python", "-u", "bot.py"]