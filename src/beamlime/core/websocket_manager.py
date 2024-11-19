# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
from fastapi import WebSocket


class WebSocketManager:
    def __init__(self):
        self.active_connections: set[WebSocket] = set()
        self.is_ready = False
        self._counter = 0

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)
        self.is_ready = True

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        if not self.active_connections:
            self.is_ready = False

    async def send(self, message: dict):
        if not self.is_ready:
            return  # Silently ignore if no connections

        disconnected = set()
        for connection in self.active_connections:
            try:
                self._counter += 1
                await connection.send_bytes(message)
            except Exception:  # noqa: PERF203
                disconnected.add(connection)

        # Clean up disconnected clients
        for connection in disconnected:
            self.disconnect(connection)
