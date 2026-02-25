# Use official Python image
FROM python:3.11-slim

# Set working directory inside container
WORKDIR /app

# Pre-create runtime directories with restrictive defaults.
RUN mkdir -p /app/data /logs && chmod 700 /app/data /logs

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the bot code and env files into the container
COPY . .

EXPOSE 8080

# Run the bot
CMD ["python", "-u", "bot.py"]
