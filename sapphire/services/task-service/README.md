# Task Management Service

A FastAPI-based microservice for managing tasks within the SAPPHIRE Forecast Tools platform.

## Overview

The Task Service provides RESTful API endpoints for creating, reading, updating, and deleting tasks. It features PostgreSQL database integration, comprehensive error handling, health checks, and full test coverage.

## Features

- **CRUD Operations**: Full create, read, update, delete functionality for tasks
- **Database Integration**: PostgreSQL with SQLAlchemy ORM
- **Schema Validation**: Pydantic models for request/response validation
- **Health Checks**: Basic and readiness endpoints for monitoring
- **CORS Support**: Configurable cross-origin resource sharing
- **Pagination**: Efficient data retrieval with skip/limit parameters
- **Error Handling**: Comprehensive exception handling with meaningful error messages
- **Logging**: Structured logging for debugging and monitoring
- **Database Migrations**: Alembic for schema version control
- **Testing**: Pytest with coverage reporting

## Tech Stack

- **Framework**: FastAPI 0.118.3
- **Database**: PostgreSQL 15
- **ORM**: SQLAlchemy 2.0.44
- **Migrations**: Alembic 1.17.0
- **Validation**: Pydantic 2.12.0
- **Testing**: Pytest 8.4.2
- **ASGI Server**: Uvicorn 0.37.0

## Project Structure
```
task-service/
├── app/
│   ├── __init__.py
│   ├── main.py           # FastAPI application and endpoints
│   ├── database.py       # Database configuration and session management
│   ├── models.py         # SQLAlchemy ORM models
│   ├── schemas.py        # Pydantic schemas for validation
│   ├── crud.py           # Database CRUD operations
│   ├── config.py         # Configuration management
│   └── logger.py         # Logging configuration
├── tests/
│   ├── __init__.py
│   ├── conftest.py       # Pytest fixtures and configuration
│   ├── pytest.ini        # Pytest settings
│   ├── test_config.py    # Configuration tests
│   ├── test_crud.py      # CRUD operation tests
│   └── test_tasks.py     # API endpoint tests
├── .env                  # Environment variables (not in git)
├── .env.example          # Environment variables template
├── requirements.txt      # Python dependencies
├── Dockerfile           # Docker container definition
├── alembic.ini          # Alembic configuration
└── README.md            # This file
```

## Getting Started

### Prerequisites

- Python 3.11+
- PostgreSQL 15+
- Docker and Docker Compose (optional)

### Installation

#### Option 1: Using Docker (Recommended)
```bash
# From project root (SAPPHIRE_Forecast_Tools/)
docker-compose up -d
```

The service will be available at http://localhost:8001

#### Option 2: Local Development
```bash
# Navigate to task-service directory
cd sapphire/services/task-service

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with your configuration

# Run database migrations
alembic upgrade head

# Start the server
uvicorn app.main:app --reload --host 0.0.0.0 --port 8001
```

## Configuration

Create a `.env` file based on `.env.example`:
```bash
# Application
APP_NAME="Task Management Service"
VERSION="1.0.0"
DEBUG=false
ENVIRONMENT=development

# Database
DATABASE_URL=postgresql://postgres:password@db:5432/taskdb
TEST_DATABASE_URL=postgresql://postgres:password@db:5432/taskdb_test

# API Configuration
API_PREFIX=/api/v1
MAX_PAGE_SIZE=100
DEFAULT_PAGE_SIZE=20

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=text

# CORS Settings
CORS_ORIGINS=["http://localhost:3000","http://localhost:8080"]
CORS_CREDENTIALS=true
```

## API Endpoints

### Health Checks

#### GET /health
Basic health check

**Response:**
```json
{
  "status": "healthy",
  "service": "Task Management Service"
}
```

#### GET /health/ready
Readiness check including database connectivity

**Response:**
```json
{
  "status": "ready",
  "service": "Task Management Service",
  "database": "connected"
}
```

### Tasks

#### POST /tasks/
Create a new task

**Request Body:**
```json
{
  "title": "Complete project documentation",
  "description": "Write comprehensive README files",
  "status": "pending",
  "priority": "high"
}
```

**Response:** `201 Created`
```json
{
  "id": 1,
  "title": "Complete project documentation",
  "description": "Write comprehensive README files",
  "status": "pending",
  "priority": "high",
  "created_at": "2025-10-17T10:30:00Z",
  "updated_at": "2025-10-17T10:30:00Z"
}
```

#### GET /tasks/
Get all tasks with pagination

**Query Parameters:**
- `skip`: Number of records to skip (default: 0)
- `limit`: Maximum number of records to return (default: 100)

**Response:** `200 OK`
```json
{
  "tasks": [...],
  "total": 42,
  "skip": 0,
  "limit": 100
}
```

#### GET /tasks/{task_id}
Get a specific task by ID

**Response:** `200 OK`
```json
{
  "id": 1,
  "title": "Complete project documentation",
  "description": "Write comprehensive README files",
  "status": "completed",
  "priority": "high",
  "created_at": "2025-10-17T10:30:00Z",
  "updated_at": "2025-10-17T15:45:00Z"
}
```

#### PUT /tasks/{task_id}
Update a task

**Request Body:**
```json
{
  "title": "Updated title",
  "status": "completed"
}
```

**Response:** `200 OK`

#### DELETE /tasks/{task_id}
Delete a task

**Response:** `200 OK`
```json
{
  "message": "Task deleted successfully",
  "task_id": 1
}
```

## Interactive API Documentation

Once the service is running, access the interactive documentation:

- **Swagger UI**: http://localhost:8001/docs
- **ReDoc**: http://localhost:8001/redoc

## Testing

### Run Tests
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=app --cov-report=html

# Run specific test file
pytest tests/test_tasks.py -v

# Run tests matching pattern
pytest tests/ -k "test_create"
```

### Using Docker
```bash
# Run tests in container
docker-compose exec api pytest

# Run with coverage
docker-compose exec api pytest --cov=app --cov-report=term-missing
```

### Test Coverage

View HTML coverage report:
```bash
pytest --cov=app --cov-report=html
open htmlcov/index.html  # macOS
# or
start htmlcov/index.html  # Windows
```

## Database Migrations

Using Alembic for database schema management:
```bash
# Create a new migration
alembic revision --autogenerate -m "Add new column to tasks"

# Apply migrations
alembic upgrade head

# Rollback one migration
alembic downgrade -1

# View migration history
alembic history

# View current version
alembic current
```

### Using Docker
```bash
docker-compose exec api alembic upgrade head
docker-compose exec api alembic revision --autogenerate -m "Description"
```

## Development

### Code Style

Follow these conventions:
- PEP 8 style guide
- Type hints for function parameters and return values
- Docstrings for all functions and classes
- Maximum line length: 88 characters (Black formatter default)

### Adding New Endpoints

1. Define Pydantic schema in `schemas.py`
2. Add database model in `models.py` (if needed)
3. Implement CRUD operation in `crud.py`
4. Create endpoint in `main.py`
5. Write tests in `tests/`
6. Create migration with Alembic

### Logging

The service uses structured logging:
```python
from app.logger import logger

logger.info("Task created", extra={"task_id": task.id})
logger.error("Failed to connect to database", exc_info=True)
```

Log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL

## Troubleshooting

### Database Connection Failed
```bash
# Check if PostgreSQL is running
docker-compose ps db

# View database logs
docker-compose logs db

# Restart database
docker-compose restart db
```

### Migration Errors
```bash
# Reset database (WARNING: destroys all data)
docker-compose down -v
docker-compose up -d db
alembic upgrade head
```

### Import Errors
```bash
# Ensure PYTHONPATH is set correctly
export PYTHONPATH=/path/to/task-service

# Or run from project root
cd SAPPHIRE_Forecast_Tools
python -m sapphire.services.task-service.app.main
```

## Performance Considerations

- Database connection pooling is configured in `database.py`
- Use pagination for large result sets
- Indexes are defined in database models
- Consider caching for frequently accessed data

## Security

- Never commit `.env` files with real credentials
- Use environment variables for sensitive configuration
- Implement authentication/authorization (planned)
- Validate all input with Pydantic schemas
- Use parameterized queries (SQLAlchemy handles this)

## Monitoring

Health check endpoints for monitoring systems:
- `/health` - Basic liveness check
- `/health/ready` - Readiness check with database connectivity

## Future Enhancements

- [ ] Authentication and authorization integration
- [ ] Task assignment to users
- [ ] Task categories and tags
- [ ] Due dates and reminders
- [ ] File attachments
- [ ] Task comments and activity log
- [ ] Search and filtering
- [ ] Bulk operations
- [ ] WebSocket support for real-time updates
- [ ] Caching layer (Redis)
- [ ] Rate limiting
- [ ] Metrics and monitoring (Prometheus)

## Contributing

1. Create a feature branch
2. Write tests for new functionality
3. Ensure all tests pass
4. Update documentation
5. Submit a pull request

## License

[Specify license]

## Support

For issues and questions, please contact [your team/support email]