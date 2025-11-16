from datetime import datetime
from trade_mgmt import TradeManager
from src.log_system import LogManager
from src.production import process_message
import websockets, asyncio, sys, logging, signal, os
from src.server import ServerManager, ModelManager, handler

shutdown_event = asyncio.Event()

def handle_sigint(signum, frame):
    """Evita que Ctrl+C cierre el loop directamente."""
    logging.warning("SIGINT recibido en servidor. Ignorando (main controla apagado).")

signal.signal(signal.SIGINT, handle_sigint)

async def main():
    server_port = int(sys.argv[1])

    state = ServerManager()
    logSystem = LogManager()
    monitoring = TradeManager()
    model_manager = ModelManager("production_models")

    logSystem.initialInterval()
    model_manager.load_models_from_folder()

    async with websockets.serve(
        lambda ws, path: handler(ws, path, state, monitoring, model_manager, process_message),
        "localhost", server_port,
    ):
        logging.info(f"Servidor WebSocket corriendo en localhost:{server_port}")

        # tareas en segundo plano
        tasks = [
            asyncio.create_task(state.heartbeat(), name="heartbeat"),
            asyncio.create_task(logSystem.schedulerLogTask(monitoring), name="logTask")]

        try:
            # bucle principal: esperar a shutdown.flag
            while not os.path.exists("shutdown.flag"):
                await asyncio.sleep(1)

            logging.info("Apagado detectado, cerrando servidor limpiamente...")

        finally:
            if monitoring.db_trades:
                print("Guardando trades existentes")
                await logSystem.logTrades(monitoring, datetime.now())
            else:
                print("Sin trades por guardar")

            # Cancelar todas las tareas en segundo plano de forma controlada
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

            logging.info("Servidor detenido correctamente.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # evitar traceback por Ctrl+C
        pass
