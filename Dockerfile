# Use official Python image
FROM python:3.11-slim

# Set working directory inside container
WORKDIR /app

# Refresh base OS packages so security fixes from the Debian slim image
# are applied even when the parent image lags behind the latest point release.
RUN apt-get update \
  && apt-get install -y --no-install-recommends --only-upgrade \
    libc-bin \
    libc6 \
  && rm -rf /var/lib/apt/lists/*

# Pre-create runtime directories with restrictive defaults.
RUN mkdir -p /app/data /logs && chmod 700 /app/data /logs

# Install dependencies
COPY requirements.txt .
RUN python -m pip install --no-cache-dir --upgrade \
    pip \
    "setuptools>=78.1.1" \
    "cryptography>=46.0.5" \
    "wheel>=0.46.2" \
    "jaraco.context>=6.1.0" \
  && python -m pip install --no-cache-dir -r requirements.txt

# Copy the bot code and env files into the container
COPY . .

EXPOSE 8080 8081

# Run the bot
CMD ["python", "-u", "bot.py"]
