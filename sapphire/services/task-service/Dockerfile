FROM python:3.11-slim

WORKDIR /code

# Copy requirements and install
COPY services/task-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy task-service code
COPY services/task-service/app ./app

# Copy tests directory
COPY services/task-service/tests ./tests

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]
