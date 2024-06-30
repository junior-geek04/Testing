import pika
import json
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from myapi import Base, User
from dotenv import load_dotenv
import os
from loguru import logger

load_dotenv()
SQLALCHEMY_DATABASE_URL = os.getenv('DATABASE_URL')
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Database operations
def get_user_by_id(db, user_id):
    return db.query(User).filter(User.id == user_id).first()

# Callback function for message processing
def callback(ch, method, properties, body):
    try:
        message = json.loads(body)
        user_id = message['id']
        callback_url = message['callback_url']

        db = SessionLocal()
        user = get_user_by_id(db, user_id)

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
                logger.info(f"Successfully sent data to {callback_url}")
                ch.basic_ack(delivery_tag=method.delivery_tag)  # Manual acknowledgment
                return {"message": f"Successfully processed message for user ID {user_id}"}
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to send data to {callback_url}: {str(e)}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)  # Reject and discard message
                # Send message to Dead Letter Exchange (DLX) on failure
                send_to_dl_exchange(body)
                return {"message": f"Failed to send data to {callback_url}: {str(e)}"}
        else:
            logger.warning(f"User with ID {user_id} not found.")
            return {"message": f"User with ID {user_id} not found."}
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)  # Reject and discard message
        # Send message to Dead Letter Exchange (DLX) on exception
        send_to_dl_exchange(body)
        return {"message": f"Error processing message: {str(e)}"}
    finally:
        db.close()


def send_to_dl_exchange(body):
    try:
        dl_connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        dl_channel = dl_connection.channel()

        dl_exchange_name = 'dlx'
        dl_queue_name = 'dl_queue'

        dl_channel.exchange_declare(exchange=dl_exchange_name, exchange_type='direct')

        # Declare and bind the DL queue to the DLX
        dl_channel.queue_declare(queue=dl_queue_name)
        dl_channel.queue_bind(exchange=dl_exchange_name, queue=dl_queue_name, routing_key='')

        # Publish message to DLX
        dl_channel.basic_publish(exchange=dl_exchange_name, routing_key='', body=body)

        logger.info(f"Message sent to Dead Letter Exchange (DLX): {body}")

        dl_connection.close()
    except Exception as e:
        logger.error(f"Error sending message to DLX: {str(e)}")

# Establish RabbitMQ connection and consume messages
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='user_queue')

# Implementing QoS to limit number of unacknowledged messages
channel.basic_qos(prefetch_count=1)

# Set up consumer with manual acknowledgment
channel.basic_consume(queue='user_queue', on_message_callback=callback)

logger.info('Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
