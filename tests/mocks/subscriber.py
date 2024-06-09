import asyncio
import typing

import msgspec
from fastapi import FastAPI, WebSocket

app = FastAPI()

# Dictionary to store subscriptions and their corresponding WebSocket connections
subscriptions: typing.Dict[str, WebSocket] = {}

@app.websocket("/ws/subscriber")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            data = await websocket.receive_bytes()
            message = msgspec.msgpack.decode(data)
            if message.get('id'):
                subscriptions[message['id']] = websocket
                asyncio.create_task(send_notifications(message['id']))
                print(f"Subscription added: {message['id']}")
    except Exception as e:
        print("Exception:", e)
    finally:
        await websocket.close()

async def send_notification(subscription_id: str, notification: typing.Dict[str, typing.Any]):
    if subscription_id in subscriptions:
        websocket = subscriptions[subscription_id]
        await websocket.send_bytes(msgspec.msgpack.encode(notification))
    else:
        print(f"No subscriber found for subscription ID: {subscription_id}")

async def send_notifications(subscription_id = '1'):
    while True:
        await asyncio.sleep(5)
        notification = {'id': subscription_id, 'content': 'Notification 2'}
        await send_notification(subscription_id, notification)
