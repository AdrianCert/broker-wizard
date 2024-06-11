import asyncio
import typing

import msgspec
import websockets


class Publisher:
    def __init__(self, server_uri: str):
        self.server_uri = server_uri
        self.websocket = None

    async def connect(self):
        self.websocket = await websockets.connect(self.server_uri)

    async def publish(self, message: typing.Dict[str, typing.Any]):
        if self.websocket is None or self.websocket.closed:
            await self.connect()

        payload = ("publish", {"message": message})
        await self.websocket.send(msgspec.msgpack.encode(payload))

# Example usage
async def main():
    publisher = Publisher("ws://localhost:8900/ws")

    # Publish a message
    await publisher.publish({'type': 'news', 'priority': 'high', 'content': 'Breaking news!!'})

    # Simulate dynamic publishing
    # await asyncio.sleep(5)
    # await publisher.publish({'type': 'weather', 'region': 'north', 'content': 'Sunny weather'})

asyncio.run(main())
