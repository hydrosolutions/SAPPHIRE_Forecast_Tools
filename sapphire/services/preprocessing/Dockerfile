FROM python:3.11-slim

WORKDIR /code

# Copy requirements and install
COPY services/preprocessing/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy data-service code
COPY services/preprocessing/app ./app

# Expose port
EXPOSE 8002

# Run the application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8002"]