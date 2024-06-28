from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, Request
from pydantic import BaseModel, EmailStr
from typing import Dict
from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, JSON, and_, create_engine, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import pika
import json
from dotenv import load_dotenv
import requests,os


load_dotenv()
# Database setup
SQLALCHEMY_DATABASE_URL = os.getenv('DATABASE_URL')
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# URL for callback endpoint
BASE_URL = 'http://localhost:8000'
callback_url = f"{BASE_URL}/callback"

# User model definition
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(50), index=True)
    age = Column(Integer)
    md = Column(JSON)
    email = Column(String(50), unique=True, nullable=False)
    created_date = Column(DateTime(timezone=True), server_default=func.now())
    modify_date = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


Base.metadata.create_all(bind=engine)

# Req Res Validation model
class UserBase(BaseModel):
    name: str
    age: int
    md: Dict
    email: EmailStr

class UserCreate(UserBase):
    pass

class UserUpdate(UserBase):
    id: int

class UserResponse(UserBase):
    id: int
    created_date: datetime
    modify_date: datetime

    class Config:
        orm_mode = True

# CRUD operations
def get_user_by_email(db: Session, email: str):
    try:
        return db.query(User).filter(User.email == email).first()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving user by email: {str(e)}")

def get_user_by_id(db: Session, user_id: int):
    try:
        return db.query(User).filter(User.id == user_id).first()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving user by ID: {str(e)}")

def create_user(db: Session, user: UserCreate):
    try:
        db_user = User(
            name=user.name,
            age=user.age,
            md=user.md,
            email=user.email
        )
        db.add(db_user)
        db.commit()
        db.refresh(db_user)
        return db_user
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating user: {str(e)}")

def update_user(db: Session, user: UserUpdate):
    try:
        db_user = db.query(User).filter(User.id == user.id).first()
        if db_user:
            db_user.name = user.name
            db_user.age = user.age
            db_user.md = user.md
            db.commit()
            db.refresh(db_user)
        return db_user
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating user: {str(e)}")


app = FastAPI()

# Dependency for getting database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Function to send data to RabbitMQ
def send_to_rabbitmq(id: int, callback_url: str):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='user_queue')
        message = json.dumps({"id": id, "callback_url": callback_url})
        channel.basic_publish(exchange='', routing_key='user_queue', body=message)
        connection.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error sending message to RabbitMQ: {str(e)}")

# Create a user
@app.post("/users/create", response_model=UserResponse)
def create_user_endpoint(user: UserCreate, db: Session = Depends(get_db)):
    if not user.email:
        raise HTTPException(status_code=400, detail="Email field is required")
    db_user = get_user_by_email(db, email=user.email)
    if db_user:
        raise HTTPException(status_code=400, detail=f"Email {user.email} already exists. User data: {db_user}")
    return create_user(db=db, user=user)

# Read a user by email
@app.get("/users", response_model=UserResponse)
def read_user(email: str, db: Session = Depends(get_db)):
    if not email:
        raise HTTPException(status_code=400, detail="Email field is required")
    db_user = get_user_by_email(db, email=email)
    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return db_user

# Update a user
@app.put("/users/update", response_model=UserResponse)
def update_user_endpoint(user: UserUpdate, db: Session = Depends(get_db)):
    if not user.id or not user.email:
        raise HTTPException(status_code=400, detail="Both id and email fields are required")
    
    db_user = db.query(User).filter(and_(User.id == user.id, User.email == user.email)).first()
    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")
    
    return update_user(db=db, user=user)

# Send data to RabbitMQ queue
@app.post("/send_to_queue", status_code=202)
def send_to_queue(id: int, callback_url: str, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    if not id:
        raise HTTPException(status_code=400, detail="ID field is required")    
    user = get_user_by_id(db, user_id=id)
    
    background_tasks.add_task(send_to_rabbitmq, id, callback_url)
    return {"message": "User data has been sent to RabbitMQ"}

# Callback endpoint to receive data and log it
@app.post("/callback")
async def callback_endpoint(request: Request):
    try:
        data = await request.json()
        with open("log.txt", "a") as log_file:
            log_file.write(f"Received data: {json.dumps(data)}\n")
        return {"message": "Data received"}
    except Exception as e:
        print(f"Failed to process data: {str(e)}")
        with open("log.txt", "a") as log_file:
            log_file.write(f"Failed to process data: {str(e)}\n")
        raise HTTPException(status_code=400, detail="Failed to process data")
