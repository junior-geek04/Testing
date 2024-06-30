# FastAPI User Management with RabbitMQ and MySQL

This project is a user management system built with FastAPI, integrating RabbitMQ for message queuing and MySQL for database management.



### 1. Clone the repository

```sh
git clone https://github.com/junior-geek04/Testing.git
cd Testing
```

### 2. Create and Activate Virtual Environment

```sh
python -m venv venv
source venv\Scripts\activate
```

### 3. Install required packages

```sh
pip install -r requirements.txt
```

### 4. Start the FastAPI server

```sh
uvicorn myapi:app --reload
```

### 5. Start RabbitMQ server

```sh
rabbitmq-server.bat
```
### 6. Start the RabbitMQ consumer

```sh
python consumer.py
```

## Endpoints Create a User
```sh
URL: /users/create
Method: POST
Request Body
```

## Read a User by Email
```sh
URL: /users
Method: GET
Query Parameters: email
Response: User data.
```
## Update a User
```sh
URL: /users/update
Method: PUT
```

## Send User Data to Queue
```sh
URL: /send_to_queue
Method: POST
```

## Callback Endpoint
```sh
URL: /callback
Method: POST
Request Body: User data (auto-sent by the consumer).
Response: Confirmation message.
```


