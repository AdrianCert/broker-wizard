import asyncio
import collections
import contextlib
import dataclasses
import os
import time
import typing
import uuid
from pathlib import Path

import msgspec
import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from broker_wizard.tools import logging

default = object()
MAX_JOB_CYCLES = 0xFFFFFFFF
# from typing import Any, Dict, List
# import uvicorn


class Types:
    SubscriptionId = str
    ConnectionId = str
    FieldName = str
    Criteria = typing.Tuple[str, typing.Any]


class MatchEngine:
    alias = {
        "eq": "equal",
        "==": "equal",
    }

    def match_equal(self, data: str, target: str) -> bool:
        return data == target

    def match(self, data: typing.Any, criteria: str, **params) -> bool:
        if criteria in self.alias:
            criteria = self.alias[criteria]

        match_handler = getattr(self, f"match_{criteria}", None)
        if not match_handler:
            logging.error("Invalid criteria: %s", criteria)
            return False
        return match_handler(data, **params)


class SubscriptionsManager:
    subscriptions_table: typing.Dict[Types.SubscriptionId, Types.ConnectionId]
    interests_table: typing.Dict[
        Types.FieldName, typing.Dict[Types.SubscriptionId, Types.Criteria]
    ]

    def __init__(self):
        self.subscriptions_table = {}
        self.interests_table = {}
        self.match_engine = MatchEngine()

    def add_subscription(
        self,
        connection_id: Types.ConnectionId,
        subscription: typing.Dict[str, Types.Criteria],
        subscription_id: Types.SubscriptionId = default,
    ):
        if subscription_id is default:
            subscription_id = str(uuid.uuid4())

        self.subscriptions_table[subscription_id] = connection_id
        for field_name, field_value in subscription.items():
            if field_name not in self.interests_table:
                self.interests_table[field_name] = {}
            criteria = field_value
            if isinstance(field_value, str):
                criteria = ("equal", {"target": field_value})
            self.interests_table[field_name][subscription_id] = criteria

        return subscription_id

    def remove_subscription(self, subscription_id: Types.SubscriptionId):
        connection_id = self.subscriptions_table.pop(subscription_id)
        for field_name, field_value in self.interests_table.items():
            if subscription_id in field_value:
                field_value.pop(subscription_id)
        return connection_id

    def group_subscriptions(self, subscription_ids: typing.List[Types.SubscriptionId]):
        grouped_subscriptions: typing.Dict[Types.ConnectionId, typing.List[str]] = {}
        for subscription_id in subscription_ids:
            connection_id = self.subscriptions_table.get(subscription_id)
            if connection_id not in grouped_subscriptions:
                grouped_subscriptions[connection_id] = []
            grouped_subscriptions[connection_id].append(subscription_id)
        return grouped_subscriptions

    def match_subscription(
        self, data: typing.Dict[str, typing.Any]
    ) -> typing.List[Types.SubscriptionId]:
        matching_subscriptions: typing.Set[Types.SubscriptionId] = set(
            self.subscriptions_table
        )
        dismissed_subscriptions: typing.Set[Types.SubscriptionId] = set()
        for field_name, field_value in data.items():
            if field_name not in self.interests_table:
                continue
            for subscription_id, criteria in self.interests_table[field_name].items():
                criteria_type, criteria_value = criteria
                if subscription_id in dismissed_subscriptions:
                    continue

                if isinstance(criteria_value, str):
                    criteria_value = {"target": criteria_value}

                if not self.match_engine.match(
                    field_value, criteria_type, **criteria_value
                ):
                    dismissed_subscriptions.add(subscription_id)
                    matching_subscriptions.discard(subscription_id)

        return list(matching_subscriptions)


@dataclasses.dataclass
class MessageTask:
    message: bytes
    connection_id: str
    message_id: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))
    send_attempts: int = 0
    lifetime: int = 60
    status: str = "pending"
    send_time: float = dataclasses.field(default_factory=lambda: time.time())


class WebSocketConnection:
    def __init__(
        self, websocket: typing.Union[WebSocket, websockets.WebSocketServerProtocol]
    ):
        self.websocket = websocket
        if isinstance(websocket, WebSocket):
            self._type = "client"
        elif isinstance(websocket, websockets.WebSocketServerProtocol):
            self._type = "server"
        else:
            self._type = "server"

    async def accept(self):
        await self.websocket.accept()

    def is_closed(self):
        if self._type == "server":
            _state = self.websocket.state
        elif self._type == "client":
            _state = self.websocket.client_state
        return _state.name in ["DISCONNECTED", "CLOSING", "CLOSED"]

    async def receive_bytes(self) -> bytes:
        if self._type == "client":
            return await self.websocket.receive_bytes()
        elif self._type == "server":
            return await self.websocket.recv()

    async def close(self):
        if not self.is_closed():
            await self.websocket.close()

    async def send_bytes(self, message: bytes):
        if self._type == "client":
            await self.websocket.send_bytes(message)
        elif self._type == "server":
            await self.websocket.send(message)


class ConnectionManager:
    connection_table: typing.Dict[Types.ConnectionId, WebSocketConnection]
    aliases: typing.Dict[Types.ConnectionId, str]
    aliases_lookup: typing.Dict[str, Types.ConnectionId]

    def __init__(self, action_handlers: typing.Dict[str, typing.Callable] = None):
        self.connection_table = {}
        self.action_handlers = action_handlers or {}
        self.aliases = {}
        self.aliases_lookup = {}
        self.send_queue = collections.deque()
        self.send_queue_lock = asyncio.Lock()
        self.send_queue_max_size = 1000
        self.send_max_attempts = 3

    async def ping(self, connection_id: str):
        websocket = self.connection_table.get(connection_id)
        if websocket:
            await websocket.send_bytes(msgspec.msgpack.encode(("ping", {})))

    async def loop(self, connection_id: str):
        websocket: WebSocketConnection = self.connection_table.get(connection_id)
        try:
            while True:
                data = await websocket.receive_bytes()
                action, payload = msgspec.msgpack.decode(data)

                connection_name = self.aliases.get(connection_id, connection_id)
                logging.info("receive <- %s: %s", connection_name, (action, payload))

                if action == "ping":
                    await websocket.send_bytes(
                        msgspec.msgpack.encode(
                            ("pong", {}),
                        )
                    )
                    continue
                if action == "pong":
                    continue

                await self.handle_action(action, connection_id, **payload)

        except WebSocketDisconnect:
            await self.disconnect(connection_id)

    async def job(self):
        job_cycles = 0
        while True:
            job_cycles += 1
            await asyncio.sleep(1)
            async with self.send_queue_lock:
                # Remove expired tasks
                all_tasks = [
                    task
                    for task in self.send_queue
                    if task.status == "pending"
                    and task.send_attempts < self.send_max_attempts
                    and time.time() - task.send_time < task.lifetime
                ][: self.send_queue_max_size]
                self.send_queue.clear()

            if job_cycles % 30 == 0:
                await asyncio.gather(
                    *[
                        self.ping(connection_id)
                        for connection_id in self.connection_table
                    ]
                )

            if job_cycles % 60 == 0:
                logging.info("Send queue: %s", len(all_tasks))

            if job_cycles > MAX_JOB_CYCLES:
                job_cycles = 0

            for task in all_tasks:
                task: MessageTask
                if task.status == "pending":
                    websocket = self.connection_table.get(task.connection_id)
                    if websocket:
                        try:
                            await websocket.send_bytes(task.message)
                            task.status = "sent"
                        except Exception as exc:
                            logging.exception(
                                "Error sending message %s to %s: %s",
                                task.message_id,
                                task.connection_id,
                                exc,
                            )
                            task.status = "error"
                            task.send_attempts += 1

    async def run(self):
        if not hasattr(self, "job_task"):
            self.job_task = asyncio.create_task(self.job())

    async def lookup_connection(self, connection: WebSocketConnection) -> str:
        for connection_id, websocket in self.connection_table.items():
            if websocket == connection:
                return connection_id
        return ""

    async def register_alias(self, connection_id: str, alias: str):
        self.aliases[connection_id] = alias
        self.aliases_lookup[alias] = connection_id

    async def connect(
        self, websocket: WebSocket = default, url: str = "", alias: str = ""
    ) -> str:
        if websocket is default:
            try:
                websocket = await websockets.connect(url)

            except Exception as exc:
                logging.info("Failed to connect to %s: %s", url, exc)
                return ""
        else:
            await websocket.accept()
        connection = WebSocketConnection(websocket)
        connection_id = str(uuid.uuid4())
        self.connection_table[connection_id] = connection

        if alias:
            await self.register_alias(connection_id, alias)

        return connection_id

    async def disconnect(
        self,
        connection: typing.Union[Types.ConnectionId, WebSocket, WebSocketConnection],
    ):
        if isinstance(connection, str):
            websocket = self.connection_table.pop(connection)
        else:
            websocket = connection
        await websocket.close()

    async def send_single(self, message: typing.Any, connection_id: str):
        connection_name = self.aliases.get(connection_id, connection_id)
        logging.info("send -> %s: %s", connection_name, message)
        self.send_queue.append(
            MessageTask(
                message=msgspec.msgpack.encode(message),
                connection_id=connection_id,
            )
        )

    async def send_broadcast(self, message: typing.Any):
        await self.send_multiple(message, list(self.connection_table))

    async def send_multiple(
        self, message: typing.Any, connection_ids: typing.List[str]
    ):
        logging.info("send -> %s: %s", ",".join(connection_ids), message)
        encoded_message = msgspec.msgpack.encode(message)
        self.send_queue.extend(
            [
                MessageTask(
                    message=encoded_message,
                    connection_id=connection_id,
                )
                for connection_id in connection_ids
            ]
        )

    async def handle_action(
        self, action: str, connection_id: Types.ConnectionId, **payload
    ):
        action_handler = self.action_handlers.get(action, None)

        if action_handler:
            try:
                await action_handler(connection_id, **payload)
            except Exception as exc:
                logging.exception("Error handling action %s: %s", action, exc)
        else:
            logging.error("Invalid action: %s", action)


class PubSubServer:
    def __init__(self, neighbors_nodes: typing.List[str] = None):
        self.connection_manager = ConnectionManager(
            action_handlers={
                "publish": self.handle_publish,
                "subscribe": self.handle_subscribe,
            }
        )
        self.subscriptions_manager = SubscriptionsManager()

        self.neighbors_nodes = neighbors_nodes or {}
        self.neighbors_connections = {}
        self.neighbors_handlers = {}

    async def connect_neighbors(self):
        for node_name in list(self.neighbors_nodes):
            if node_name in self.neighbors_connections:
                # TODO check if connection is still alive
                continue

            node_url = self.neighbors_nodes[node_name]
            if self.neighbors_nodes.get("self") == node_url:
                self.neighbors_connections[node_name] = "self"
                continue

            if not node_url or not node_url.startswith("ws"):
                logging.error("Invalid node URL: %s", node_name)
                continue

            connection_id = await self.connection_manager.connect(
                url=node_url, alias=node_name
            )

            if not connection_id:
                logging.warning("Failed to connect to %s", node_name)
                continue

            self.neighbors_handlers[node_name] = asyncio.create_task(
                self.connection_manager.loop(connection_id)
            )
            self.neighbors_connections[node_name] = connection_id
            await self.connection_manager.send_single(
                ("ping", {}), connection_id=connection_id
            )
            await asyncio.gather(
                *[
                    self.forward_subscription(subscription_id, connection_id)
                    for subscription_id in self.subscriptions_manager.subscriptions_table
                ]
            )

            logging.info("Connected to %s: %s", node_name, connection_id)

    async def job(self):
        job_cycles = 0
        while True:
            await asyncio.sleep(5)
            job_cycles += 1

            if job_cycles % 10 == 0:
                logging.info(
                    "Neighbors: %s",
                    set(self.neighbors_connections).difference(["self"]),
                )
                if len(self.neighbors_connections) < len(self.neighbors_nodes):
                    await self.connect_neighbors()

            if job_cycles > MAX_JOB_CYCLES:
                job_cycles = 0

    async def run(self):
        await self.connection_manager.run()
        await self.connect_neighbors()
        asyncio.create_task(self.job())

    async def send_message(self, message: typing.Any, websocket: WebSocket):
        await websocket.send_bytes(msgspec.msgpack.encode(message))

    async def handle_websocket(self, websocket: WebSocket):
        connection_id = await self.connection_manager.connect(websocket)
        current_url = websocket.url
        self.neighbors_nodes["self"] = current_url
        self.neighbors_connections["self"] = "self"
        await self.connection_manager.loop(connection_id)

    async def handle_publish(
        self,
        connection_id: Types.ConnectionId,
        message: typing.Any,
        extra: typing.Any = default,
    ) -> None:
        logging.info("handle publish: %s", message)

        if not isinstance(message, dict):
            logging.error("Invalid message: %s", message)
            return
        if not message:
            logging.error("Empty message")
            return

        if extra is default:
            extra = {}

        all_subscriptions = (
            extra["subscriptions"]
            if "subscriptions" in extra
            else self.subscriptions_manager.match_subscription(message)
        )

        for (
            connection_id,
            subscription_ids,
        ) in self.subscriptions_manager.group_subscriptions(all_subscriptions).items():
            extra["subscriptions"] = subscription_ids
            payload = ("publish", {"message": message, "extra": extra})

            await self.connection_manager.send_single(
                payload, connection_id=connection_id
            )

    async def handle_subscribe_confirmation(
        self, connection_id: Types.ConnectionId, subscription_id: Types.SubscriptionId
    ):
        logging.info(
            "subscription confirmation: %s <- %s", subscription_id, connection_id
        )

    async def forward_subscription(
        self, subscription_id: Types.SubscriptionId, connection_id: Types.ConnectionId
    ):
        subscription = {}
        for (
            field_name,
            field_value,
        ) in self.subscriptions_manager.interests_table.items():
            if subscription_id in field_value:
                subscription[field_name] = field_value[subscription_id]

        payload = (
            "subscribe",
            {"subscription": subscription, "subscription_id": subscription_id},
        )
        await self.connection_manager.send_single(payload, connection_id=connection_id)

    async def handle_subscribe(
        self,
        connection_id: Types.ConnectionId,
        subscription: typing.Dict[str, Types.Criteria],
        subscription_id: str = default,
    ) -> None:
        if subscription_id in self.subscriptions_manager.subscriptions_table:
            logging.warning("Subscription already exists: %s", subscription_id)
            return

        subscription_id = self.subscriptions_manager.add_subscription(
            connection_id, subscription, subscription_id=subscription_id
        )
        for neighbor_name, neighbor_connection_id in self.neighbors_connections.items():
            if neighbor_connection_id == "self":
                continue
            payload = (
                "subscribe",
                {"subscription": subscription, "subscription_id": subscription_id},
            )

            await self.connection_manager.send_single(
                payload, connection_id=neighbor_connection_id
            )

        payload = ("subscribe_confirmation", {"subscription_id": subscription_id})
        await self.connection_manager.send_single(payload, connection_id=connection_id)


class Configuration:
    def __init__(self, **kwargs):
        self.config = kwargs
        logging.info("Configuration: %s", self.config)

    def get(self, key, default=None):
        return self.config.get(key, default)

    def set(self, key, value):
        self.config[key] = value

    def update(self, **kwargs):
        self.config.update(kwargs)

    @classmethod
    def load(cls, path: str, key: str = None):
        pass
        filepath = Path(path)
        if not filepath.exists():
            return cls()

        config = msgspec.yaml.decode(filepath.read_bytes())
        config = config.get("broker_wizard", {})
        if key:
            config = config.get(key, {})

        return cls(**config)


SERVER_CONFIG = Configuration.load(
    os.getenv(
        "BROKER_WIZARD_CONFIG_PATH",
        "config.yaml",
    ),
    os.getenv("BROKER_WIZARD_CONFIG_KEY", "node-1"),
)

pubsub_server = PubSubServer(neighbors_nodes=SERVER_CONFIG.get("neighbors_nodes", {}))


async def main():
    await pubsub_server.run()


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    await pubsub_server.run()
    yield


app = FastAPI(lifespan=lifespan, title="Broker Wizard", version="0.1.0")


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await pubsub_server.handle_websocket(websocket)


if __name__ == "__main__":
    import uvicorn

    # uvicorn.run(app, host="127.0.0.1", port=8900)
    uvicorn.run(
        app,
        host=SERVER_CONFIG.get("host", "127.0.0.1"),
        port=SERVER_CONFIG.get("port", 8900),
    )
