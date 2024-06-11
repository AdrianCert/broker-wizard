import asyncio
import uuid
from typing import Any, Callable, Dict

import msgspec
import websockets

default = object()

class Subscriber:
    def __init__(self, server_uri: str):
        self.server_uri = server_uri
        self.websocket = None
        self.subscription_callbacks = {}

    async def connect(self):
        self.websocket = await websockets.connect(self.server_uri)

    async def subscribe(self, subscription: Dict[str, str], callback: Callable[[Dict[str, Any]], None], subscription_id: str = default):
        if self.websocket is None or self.websocket.closed:
            await self.connect()

        if subscription_id is default:
            subscription_id = str(uuid.uuid4())
        self.subscription_callbacks[subscription_id] = callback

        payload = ("subscribe", {
            "subscription": subscription,
            "subscription_id": subscription_id
        })
        await self.websocket.send(msgspec.msgpack.encode(payload))

    async def handle_notification(self, message: Dict[str, Any], extra: Dict[str, Any] = {}):
        for subscription_id in extra.get("subscriptions", []):
            callback = self.subscription_callbacks.get(subscription_id)
            if callback:
                callback(message)

    async def listen_for_notifications(self):
        try:
            while True:
                data = await self.websocket.recv()
                action, payload = msgspec.msgpack.decode(data)
                if action == "publish":
                    await self.handle_notification(**payload)
                if action == "ping":
                    await self.websocket.send(msgspec.msgpack.encode(("pong", {})))
        except websockets.exceptions.ConnectionClosed:
            print("Connection closed")
            self.websocket = None

    async def run(self):
        while True:
            if self.websocket is None or self.websocket.closed:
                await self.connect()
            await self.listen_for_notifications()
            await asyncio.sleep(1)

# Example usage
async def main():
    subscriber = Subscriber("ws://localhost:8901/ws")

    def news_callback(notification):
        print(f"News callback received notification: {notification}")

    def weather_callback(notification):
        print(f"Weather callback received notification: {notification}")

    # Subscribe with first criteria and callback
    await subscriber.subscribe({'type': 'news', 'priority': 'high'}, news_callback, subscription_id="1")

    # Simulate dynamic subscriptions with a different callback
    await asyncio.sleep(5)
    await subscriber.subscribe({'type': 'weather', 'region': 'north'}, weather_callback, subscription_id="2")

    await subscriber.run()

asyncio.run(main())
