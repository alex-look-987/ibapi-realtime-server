import numpy as np
import pandas as pd
import asyncio, warnings, logging
from src.features import features_async
from trade_mgmt import TradeManager, Trade
from src.server import ServerManager, ModelManager
from src.client import recv_and_parse, update_dataframe, dataframeMgmt

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO, format="%(message)s")

websockets_loggers = ["websockets.server", "websockets.protocol", "websockets.client"]
for logger_name in websockets_loggers: logging.getLogger(logger_name).setLevel(logging.WARNING)

# ------------------------------------------------ Zona Producción -------------------------------------------------------- #

async def process_message(websocket, path: str, state: ServerManager, monitoring: TradeManager, model_manager: ModelManager):
    """Procesa mensajes entrantes según el path."""

    if path == "/ibapi_end":
        try:
            data = await recv_and_parse(websocket)

            df = dataframeMgmt(data, state)

            logging.info("\nSERVER RUNNING!\n")

            df = await features_async(df)
            cols = [col for col in df.columns if 'mean' in col]

            logging.info(df.tail(25)[cols])

            await asyncio.sleep(1)
            state.end_event.set()

        except Exception:
            logging.exception("Error procesando /ibapi_end")
            state.end_event.clear()

    elif path.startswith("/ibapi_update"):
        await state.end_event.wait()

        try:
            data = await recv_and_parse(websocket)
            df = await update_dataframe(state, data)
        
            last_index = df.index[-1] if pd.notna(df.index[-1]) else None

            filter_nan = pd.notna(last_index)
            candle_state = state.current_candle is not None
            new_candle = last_index != state.current_candle

            if filter_nan:
                logging.info(df[['open', 'high', 'low', 'close']].iloc[[-1]])
            
            # NEW CANDLE INCOMING
            if all([new_candle, candle_state, filter_nan]):
                logging.info(f"new candle:{state.current_candle}")

                df = await features_async(df)
                cols = [col for col in df.columns if 'mean' in col]
                logging.info(df.tail(25)[cols])

                state.current_candle = last_index
    
            state.current_candle = last_index

            # --------------------------------- TRADES MONITORING -------------------------------- # 
            if monitoring.active_trades:
                await monitoring.monitor_active_trades(df.iloc[-1])

                historical_trades = monitoring.historicalTrades()

                logging.info(historical_trades)

        except Exception:
            logging.exception("Error procesando /ibapi_update")

        finally:
            state.end_event.clear()
            state.end_event.set()
