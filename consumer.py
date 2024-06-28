import pika
import json
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from myapi import Base, User

SQLALCHEMY_DATABASE_URL = "mysql+pymysql://root:1234@localhost:3306/userapp"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

#Finding user
def get_user_by_id(db, user_id):
    return db.query(User).filter(User.id == user_id).first()

#Callback function called when msg received
def callback(ch, method, properties, body):
    message = json.loads(body)
    user_id = message['id']
    callback_url = message['callback_url']

    db = SessionLocal()
    user = get_user_by_id(db, user_id)
    db.close()

    if user:
        user_data = {
            "id": user.id,
            "name": user.name,
            "age": user.age,
            "md": user.md,
            "email": user.email,
            "created_date": user.created_date.isoformat(),
            "modify_date": user.modify_date.isoformat(),
        }
        try:
            response = requests.post(callback_url, json=user_data)
            response.raise_for_status()  
        except requests.exceptions.RequestException as e:
            print(f"Failed to send data to {callback_url}: {str(e)}")
    else:
        print(f"User with ID {user_id} not found.")

#building connection
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
#Queue declaration
channel.queue_declare(queue='user_queue')

channel.basic_consume(queue='user_queue', on_message_callback=callback, auto_ack=True)

print('Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
