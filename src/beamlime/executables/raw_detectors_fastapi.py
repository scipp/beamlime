# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2024 Scipp contributors (https://github.com/scipp)
import asyncio

import uvicorn
from fastapi import FastAPI, WebSocket

from beamlime.core.websocket_manager import WebSocketManager
from beamlime.executables.show_detector import (
    make_arg_parser,
    make_factory,
    run_show_detector,
)

app = FastAPI()
factory = make_factory()
manager = factory[WebSocketManager]


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            _ = await websocket.receive_text()
            await asyncio.sleep(0.1)
    except Exception:
        manager.disconnect(websocket)


@app.post("/api/control")
async def control_endpoint(command: dict):
    # TODO
    return {"status": "success", "command": command}


async def start_uvicorn():
    config = uvicorn.Config(app, host="127.0.0.1", port=5555)
    server = uvicorn.Server(config)
    await server.serve()


async def run_as_thread(*args, **kwargs):
    return await asyncio.to_thread(run_show_detector, *args, **kwargs)


async def run_components(*args, **kwargs):
    await asyncio.gather(run_as_thread(*args, **kwargs), start_uvicorn())


def main():
    parser = make_arg_parser()
    args = parser.parse_args()
    asyncio.run(run_components(factory, args))


if __name__ == "__main__":
    main()
