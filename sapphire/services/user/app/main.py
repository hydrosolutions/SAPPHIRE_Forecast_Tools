from fastapi import FastAPI, Depends, HTTPException, status, Query
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from typing import List, Optional

from app import crud
from app.database import engine, Base, get_db
from app.schemas import (
    UserCreate, UserUpdate, UserResponse, UserInDB,
    RoleCreate, RoleResponse, RoleBulkCreate
)
from app.logger import logger
from app.config import settings

Base.metadata.create_all(bind=engine)

app = FastAPI(
    title=settings.app_name,
    version=settings.version,
    description="User Management Service for SAPPHIRE",
    docs_url="/docs",
    redoc_url="/redoc"
)

@app.get("/", tags=["Root"])
def root():
    return {
        "message": "Welcome to the User Service API",
        "docs": "/docs",
        "health": "/health"
    }

@app.get("/health", tags=["Health"])
def health_check():
    return {"status": "healthy", "service": "User Service API"}

@app.get("/health/ready", tags=["Health"])
def readiness_check(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        return {"status": "ready", "service": "User Service API", "database": "connected"}
    except Exception as e:
        logger.error(f"Readiness check failed: {str(e)}")
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Service not ready")

# ============== USER ENDPOINTS ==============
@app.post("/users/", response_model=UserResponse, status_code=status.HTTP_201_CREATED, tags=["Users"])
def create_user(user: UserCreate, db: Session = Depends(get_db)):
    """Create a new user"""
    db_user = crud.get_user_by_email(db, email=user.email)
    if db_user:
        raise HTTPException(status_code=400, detail="Email already registered")
    db_user = crud.get_user_by_username(db, username=user.username)
    if db_user:
        raise HTTPException(status_code=400, detail="Username already taken")
    try:
        return crud.create_user(db=db, user=user)
    except SQLAlchemyError as e:
        logger.error(f"Failed to create user: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to create user")

@app.get("/users/", response_model=List[UserResponse], tags=["Users"])
def read_users(
    skip: int = 0,
    limit: int = 100,
    is_active: Optional[bool] = None,
    role: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Get all users with optional filtering"""
    try:
        return crud.get_users(db, skip=skip, limit=limit, is_active=is_active, role=role)
    except SQLAlchemyError:
        raise HTTPException(status_code=500, detail="Failed to fetch users")

@app.get("/users/{user_id}", response_model=UserResponse, tags=["Users"])
def read_user(user_id: int, db: Session = Depends(get_db)):
    """Get a specific user by ID"""
    db_user = crud.get_user(db, user_id=user_id)
    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return db_user

@app.get("/users/by-email/{email}", response_model=UserInDB, tags=["Users"])
def read_user_by_email(email: str, db: Session = Depends(get_db)):
    """Get user by email (internal use for auth service)"""
    db_user = crud.get_user_by_email(db, email=email)
    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return db_user

@app.get("/users/by-username/{username}", response_model=UserInDB, tags=["Users"])
def read_user_by_username(username: str, db: Session = Depends(get_db)):
    """Get user by username (internal use for auth service)"""
    db_user = crud.get_user_by_username(db, username=username)
    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return db_user

@app.put("/users/{user_id}", response_model=UserResponse, tags=["Users"])
def update_user(user_id: int, user: UserUpdate, db: Session = Depends(get_db)):
    """Update a user"""
    db_user = crud.get_user(db, user_id=user_id)
    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")
    try:
        return crud.update_user(db=db, user_id=user_id, user=user)
    except SQLAlchemyError:
        raise HTTPException(status_code=500, detail="Failed to update user")

@app.delete("/users/{user_id}", status_code=status.HTTP_204_NO_CONTENT, tags=["Users"])
def delete_user(user_id: int, db: Session = Depends(get_db)):
    """Delete a user"""
    db_user = crud.get_user(db, user_id=user_id)
    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")
    try:
        crud.delete_user(db=db, user_id=user_id)
    except SQLAlchemyError:
        raise HTTPException(status_code=500, detail="Failed to delete user")

# ============== ROLE ENDPOINTS ==============
@app.post("/roles/", response_model=RoleResponse, status_code=status.HTTP_201_CREATED, tags=["Roles"])
def create_role(role: RoleCreate, db: Session = Depends(get_db)):
    """Create a new role"""
    db_role = crud.get_role_by_name(db, name=role.name)
    if db_role:
        raise HTTPException(status_code=400, detail="Role already exists")
    try:
        return crud.create_role(db=db, role=role)
    except SQLAlchemyError:
        raise HTTPException(status_code=500, detail="Failed to create role")

@app.get("/roles/", response_model=List[RoleResponse], tags=["Roles"])
def read_roles(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    """Get all roles"""
    return crud.get_roles(db, skip=skip, limit=limit)

@app.post("/users/{user_id}/roles/{role_id}", response_model=UserResponse, tags=["Users"])
def assign_role_to_user(user_id: int, role_id: int, db: Session = Depends(get_db)):
    """Assign a role to a user"""
    try:
        return crud.assign_role(db=db, user_id=user_id, role_id=role_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except SQLAlchemyError:
        raise HTTPException(status_code=500, detail="Failed to assign role")
