# Use official Python image
FROM python:3.11-slim

# Set working directory inside container
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the bot code and env files into the container
COPY . .

# Run the bot
CMD ["python", "bot.py"]