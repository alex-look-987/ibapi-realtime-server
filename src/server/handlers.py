from __future__ import annotations

import asyncio, logging
from websockets.server import WebSocketServerProtocol
from websockets.exceptions import ConnectionClosed, ConnectionClosedOK, ConnectionClosedError

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from trade_mgmt.manager import TradeManager
    from src.server.utils import ServerManager, ModelManager

# ---------- Handler principal ----------

async def handler(websocket: WebSocketServerProtocol, path: str,
                  state: ServerManager, monitoring: TradeManager, model_manager: ModelManager, process_message):
    """Registra clientes y mantiene la comunicación."""
    
    if path == "/ibapi_end":
        state.historical_clients.add(websocket)
        state.end_event.set()
        
    elif path.startswith("/ibapi_update"):
        df_key = path.split("_")[-1]
        state.producer_clients[df_key] = {"websocket": websocket}

    try:
        while True:
            await process_message(websocket, path, state, monitoring, model_manager)

            pong_waiter = await websocket.ping()
            await asyncio.wait_for(pong_waiter, timeout=30)

    except (ConnectionClosedOK, ConnectionClosedError, ConnectionClosed, asyncio.TimeoutError):
        logging.debug(f"Cliente desconectado: {websocket.remote_address}")

    finally:
        state.historical_clients.discard(websocket)

        state.producer_clients = {
        k: v for k, v in state.producer_clients.items()
        if v["websocket"] != websocket}

        if not websocket.closed:
            await websocket.close()