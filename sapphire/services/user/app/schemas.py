from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List
from datetime import datetime

# ============== ROLE SCHEMAS ==============
class RoleBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=50)
    description: Optional[str] = None

class RoleCreate(RoleBase):
    pass

class RoleBulkCreate(BaseModel):
    roles: List[RoleCreate]

class RoleResponse(RoleBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True

# ============== USER SCHEMAS ==============
class UserBase(BaseModel):
    email: EmailStr
    username: str = Field(..., min_length=3, max_length=100)
    full_name: Optional[str] = None

class UserCreate(UserBase):
    password: str = Field(..., min_length=8)

class UserUpdate(BaseModel):
    email: Optional[EmailStr] = None
    username: Optional[str] = Field(None, min_length=3, max_length=100)
    full_name: Optional[str] = None
    is_active: Optional[bool] = None
    password: Optional[str] = Field(None, min_length=8)

class UserResponse(UserBase):
    id: int
    is_active: bool
    is_superuser: bool
    created_at: datetime
    updated_at: Optional[datetime] = None
    roles: List[RoleResponse] = []

    class Config:
        from_attributes = True

class UserInDB(UserResponse):
    """User model with hashed password (for internal service communication)"""
    hashed_password: str

    class Config:
        from_attributes = True
