from sqlalchemy.orm import Session
from passlib.context import CryptContext
from fastapi import HTTPException
import httpx
from typing import Optional
from datetime import datetime, timedelta

from app.models import RefreshToken
from app.schemas import UserRegister
from app.config import settings

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

async def create_user_via_user_service(user_data: UserRegister) -> dict:
    """Create user via User Service"""
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"{settings.user_service_url}/users/",
                json={
                    "email": user_data.email,
                    "username": user_data.username,
                    "password": user_data.password,
                    "full_name": user_data.full_name
                }
            )
            if response.status_code == 201:
                return response.json()
            elif response.status_code == 400:
                detail = response.json().get("detail", "Registration failed")
                raise HTTPException(status_code=400, detail=detail)
            else:
                raise HTTPException(status_code=500, detail="Failed to create user")
        except httpx.RequestError:
            raise HTTPException(status_code=503, detail="User service unavailable")

async def authenticate_user(username_or_email: str, password: str) -> Optional[dict]:
    """Authenticate user via User Service"""
    async with httpx.AsyncClient() as client:
        try:
            # Try email first
            response = await client.get(f"{settings.user_service_url}/users/by-email/{username_or_email}")
            if response.status_code != 200:
                # Try username
                response = await client.get(f"{settings.user_service_url}/users/by-username/{username_or_email}")
            if response.status_code != 200:
                return None
            
            user = response.json()
            if not verify_password(password, user.get("hashed_password", "")):
                return None
            # Remove hashed_password from response
            user.pop("hashed_password", None)
            return user
        except httpx.RequestError:
            raise HTTPException(status_code=503, detail="User service unavailable")

async def update_user_password(user_id: int, new_password: str) -> bool:
    """Update user password via User Service"""
    async with httpx.AsyncClient() as client:
        try:
            response = await client.put(
                f"{settings.user_service_url}/users/{user_id}",
                json={"password": new_password}
            )
            return response.status_code == 200
        except httpx.RequestError:
            raise HTTPException(status_code=503, detail="User service unavailable")

# ============== REFRESH TOKEN MANAGEMENT ==============
def store_refresh_token(db: Session, user_id: int, token: str) -> RefreshToken:
    expires_at = datetime.utcnow() + timedelta(days=settings.refresh_token_expire_days)
    db_token = RefreshToken(user_id=user_id, token=token, expires_at=expires_at)
    db.add(db_token)
    db.commit()
    db.refresh(db_token)
    return db_token

def verify_refresh_token(db: Session, user_id: int, token: str) -> bool:
    db_token = db.query(RefreshToken).filter(
        RefreshToken.user_id == user_id,
        RefreshToken.token == token,
        RefreshToken.revoked == False,
        RefreshToken.expires_at > datetime.utcnow()
    ).first()
    return db_token is not None

def revoke_refresh_token(db: Session, user_id: int, token: str) -> None:
    db.query(RefreshToken).filter(
        RefreshToken.user_id == user_id,
        RefreshToken.token == token
    ).update({"revoked": True})
    db.commit()

def revoke_all_refresh_tokens(db: Session, user_id: int) -> None:
    db.query(RefreshToken).filter(RefreshToken.user_id == user_id).update({"revoked": True})
    db.commit()
