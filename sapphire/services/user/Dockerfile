FROM python:3.11-slim

WORKDIR /code

# Copy requirements and install
COPY services/user/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy user service code
COPY services/user/app ./app

# Expose port
EXPOSE 8004

# Run the application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8004"]