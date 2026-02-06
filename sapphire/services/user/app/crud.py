from sqlalchemy.orm import Session
from typing import Optional, List
from passlib.context import CryptContext
# import hashlib

from app.models import User, Role
from app.schemas import UserCreate, UserUpdate, RoleCreate

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def get_password_hash(password: str) -> str:
    # sha = hashlib.sha256(password.encode("utf-8")).hexdigest()
    return pwd_context.hash(password)

# ============== USER CRUD ==============
def get_user(db: Session, user_id: int) -> Optional[User]:
    return db.query(User).filter(User.id == user_id).first()

def get_user_by_email(db: Session, email: str) -> Optional[User]:
    return db.query(User).filter(User.email == email).first()

def get_user_by_username(db: Session, username: str) -> Optional[User]:
    return db.query(User).filter(User.username == username).first()

def get_users(
    db: Session,
    skip: int = 0,
    limit: int = 100,
    is_active: Optional[bool] = None,
    role: Optional[str] = None
) -> List[User]:
    query = db.query(User)
    if is_active is not None:
        query = query.filter(User.is_active == is_active)
    if role:
        query = query.join(User.roles).filter(Role.name == role)
    return query.offset(skip).limit(limit).all()

def create_user(db: Session, user: UserCreate) -> User:
    hashed_password = get_password_hash(user.password)
    db_user = User(
        email=user.email,
        username=user.username,
        hashed_password=hashed_password,
        full_name=user.full_name
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user

def update_user(db: Session, user_id: int, user: UserUpdate) -> User:
    db_user = db.query(User).filter(User.id == user_id).first()
    update_data = user.model_dump(exclude_unset=True)
    if "password" in update_data:
        update_data["hashed_password"] = get_password_hash(update_data.pop("password"))
    for field, value in update_data.items():
        setattr(db_user, field, value)
    db.commit()
    db.refresh(db_user)
    return db_user

def delete_user(db: Session, user_id: int) -> None:
    db_user = db.query(User).filter(User.id == user_id).first()
    db.delete(db_user)
    db.commit()

# ============== ROLE CRUD ==============
def get_role(db: Session, role_id: int) -> Optional[Role]:
    return db.query(Role).filter(Role.id == role_id).first()

def get_role_by_name(db: Session, name: str) -> Optional[Role]:
    return db.query(Role).filter(Role.name == name).first()

def get_roles(db: Session, skip: int = 0, limit: int = 100) -> List[Role]:
    return db.query(Role).offset(skip).limit(limit).all()

def create_role(db: Session, role: RoleCreate) -> Role:
    db_role = Role(name=role.name, description=role.description)
    db.add(db_role)
    db.commit()
    db.refresh(db_role)
    return db_role

def assign_role(db: Session, user_id: int, role_id: int) -> User:
    db_user = db.query(User).filter(User.id == user_id).first()
    db_role = db.query(Role).filter(Role.id == role_id).first()
    if not db_user:
        raise ValueError("User not found")
    if not db_role:
        raise ValueError("Role not found")
    if db_role not in db_user.roles:
        db_user.roles.append(db_role)
        db.commit()
        db.refresh(db_user)
    return db_user
