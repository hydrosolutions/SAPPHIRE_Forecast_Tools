FROM python:3.11-slim

WORKDIR /code

# Copy requirements and install
COPY services/auth/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy auth service code
COPY services/auth/app ./app

# Expose port
EXPOSE 8005

# Run the application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8005"]