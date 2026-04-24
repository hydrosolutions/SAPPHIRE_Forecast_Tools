FROM python:3.11-slim

WORKDIR /code

# Copy requirements and install
COPY services/postprocessing/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy data-service code
COPY services/postprocessing/app ./app
COPY services/postprocessing/alembic.ini ./
COPY services/postprocessing/alembic ./alembic

# Expose port
EXPOSE 8003

# Run the application
CMD ["sh", "-c", "alembic upgrade head && uvicorn app.main:app --host 0.0.0.0 --port 8003"]