import pandas as pd
from io import StringIO
import orjson, lz4.frame
from src.server import ServerManager
from miscs.candle_manager import candleBuilder
from websockets.server import WebSocketServerProtocol
from websockets.exceptions import ConnectionClosedError

# ---------- IB API Message Processing ---------- #

candle_builder = candleBuilder('15min')

async def recv_and_parse(websocket: WebSocketServerProtocol):
    """Recibe datos, descomprime y decodifica JSON."""

    try:
        compressed_data = await websocket.recv()
        decompressed_data = lz4.frame.decompress(compressed_data)
        return orjson.loads(decompressed_data)
    
    except ConnectionClosedError:
        pass

def dataframeMgmt(data, state: ServerManager):
    
    df = pd.read_json(StringIO(data['dataframe']), orient='split', precise_float=True)
    state.historical_data[data['df_key']] = df

    if data['df_key'] == "eurusd_15mins":
        state.frames[0] = data['df_key']

    if data['df_key'] in state.historical_data: 
        df = state.historical_data[data['df_key']]

    return df

async def update_dataframe(state: ServerManager, candle_data) -> pd.DataFrame:
    """Actualiza el dataframe histórico de manera segura para pandas streaming."""
    
    try:
        # DataFrame histórico
        df = pd.DataFrame(state.historical_data[state.frames[0]])
        df.index.name = 'date'

        # normalize timestamp
        ts = candle_builder.normalize_time(candle_data['date'])

        df.loc[ts] = candle_data['candle']
        
        state.historical_data[state.frames[0]] = df
        
        return df
    except:
        pass