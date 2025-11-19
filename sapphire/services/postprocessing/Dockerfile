FROM python:3.11-slim

WORKDIR /code

# Copy requirements and install
COPY services/postprocessing/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy data-service code
COPY services/postprocessing/app ./app

# Expose port
EXPOSE 8003

# Run the application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8003"]