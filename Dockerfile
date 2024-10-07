FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Specify the command to run the app
CMD ["gunicorn", "--workers", "4", "--bind", "0.0.0.0:5000", "app:app"]
