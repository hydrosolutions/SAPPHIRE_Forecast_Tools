from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy import text
from sqlalchemy.orm import Session
from datetime import timedelta

from app import crud, auth
from app.database import engine, Base, get_db
from app.schemas import (
    Token, TokenRefresh, UserRegister, UserLoginResponse,
    PasswordChange, PasswordReset, PasswordResetConfirm
)
from app.config import settings
from app.logger import logger

Base.metadata.create_all(bind=engine)

app = FastAPI(
    title=settings.app_name,
    version=settings.version,
    description="Authentication Service for SAPPHIRE",
    docs_url="/docs",
    redoc_url="/redoc"
)

@app.get("/", tags=["Root"])
def root():
    return {"message": "Welcome to the Auth Service API", "docs": "/docs", "health": "/health"}

@app.get("/health", tags=["Health"])
def health_check():
    return {"status": "healthy", "service": "Auth Service API"}

@app.get("/health/ready", tags=["Health"])
def readiness_check(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        return {"status": "ready", "service": "Auth Service API", "database": "connected"}
    except Exception as e:
        logger.error(f"Readiness check failed: {str(e)}")
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Service not ready")

@app.post("/register", response_model=UserLoginResponse, status_code=status.HTTP_201_CREATED, tags=["Authentication"])
async def register(user_data: UserRegister):
    """Register a new user"""
    try:
        user = await crud.create_user_via_user_service(user_data)
        access_token = auth.create_access_token(
            data={"sub": user["email"], "user_id": user["id"]},
            expires_delta=timedelta(minutes=settings.access_token_expire_minutes)
        )
        refresh_token = auth.create_refresh_token(data={"sub": user["email"], "user_id": user["id"]})
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer",
            "user": user
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Registration failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Registration failed")

@app.post("/login", response_model=UserLoginResponse, tags=["Authentication"])
async def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    """Login with username/email and password"""
    user = await crud.authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not user.get("is_active", True):
        raise HTTPException(status_code=400, detail="Inactive user")
    
    access_token = auth.create_access_token(
        data={"sub": user["email"], "user_id": user["id"]},
        expires_delta=timedelta(minutes=settings.access_token_expire_minutes)
    )
    refresh_token = auth.create_refresh_token(data={"sub": user["email"], "user_id": user["id"]})
    crud.store_refresh_token(db, user["id"], refresh_token)
    
    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer",
        "user": user
    }

@app.post("/refresh", response_model=Token, tags=["Authentication"])
async def refresh_token(token_data: TokenRefresh, db: Session = Depends(get_db)):
    """Get new access token using refresh token"""
    try:
        payload = auth.decode_token(token_data.refresh_token)
        email = payload.get("sub")
        user_id = payload.get("user_id")
        if not email or not user_id:
            raise HTTPException(status_code=401, detail="Invalid token")
        
        if not crud.verify_refresh_token(db, user_id, token_data.refresh_token):
            raise HTTPException(status_code=401, detail="Invalid or expired refresh token")
        
        access_token = auth.create_access_token(
            data={"sub": email, "user_id": user_id},
            expires_delta=timedelta(minutes=settings.access_token_expire_minutes)
        )
        return {"access_token": access_token, "refresh_token": token_data.refresh_token, "token_type": "bearer"}
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid refresh token")

@app.post("/logout", tags=["Authentication"])
async def logout(token_data: TokenRefresh, db: Session = Depends(get_db)):
    """Logout and invalidate refresh token"""
    try:
        payload = auth.decode_token(token_data.refresh_token)
        user_id = payload.get("user_id")
        if user_id:
            crud.revoke_refresh_token(db, user_id, token_data.refresh_token)
        return {"message": "Successfully logged out"}
    except Exception:
        return {"message": "Logged out"}

@app.get("/verify", tags=["Authentication"])
async def verify_token(current_user: dict = Depends(auth.get_current_user)):
    """Verify access token and return user info"""
    return {"valid": True, "user": current_user}

@app.post("/change-password", tags=["Authentication"])
async def change_password(
    password_data: PasswordChange,
    current_user: dict = Depends(auth.get_current_user),
    db: Session = Depends(get_db)
):
    """Change password for authenticated user"""
    user = await crud.authenticate_user(current_user["email"], password_data.current_password)
    if not user:
        raise HTTPException(status_code=400, detail="Current password is incorrect")
    await crud.update_user_password(current_user["id"], password_data.new_password)
    crud.revoke_all_refresh_tokens(db, current_user["id"])
    return {"message": "Password changed successfully"}
