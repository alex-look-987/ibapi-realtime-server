import numpy as np
import pandas as pd
from typing import Dict
from ibapi.client import *
from ibapi.wrapper import *
from miscs.ibapi_utils import get_contract
import asyncio, threading, sys, time, logging
from miscs.candle_manager import candleBuilder
from src.client import historical_end, update_candle, start_server_check

# --------------------------INITIAL CONFIG------------------------------------ #
server_port = sys.argv[1]
clientid = int(sys.argv[2])
contract_type = sys.argv[3]

port = 7496 
host = "127.0.0.1"
contract = get_contract('EURUSD')

start_server_check(f"ws://localhost:{server_port}")

# --------------------------------------------------------------------------
# IB API CLIENT
# --------------------------------------------------------------------------
class IBapi(EWrapper, EClient):
  def __init__(self, loop, inverval=3):
    EClient.__init__(self, self)
    self.data = {}
    self.reqId = 1
    self.initialSetupDone = False
    self.inverval = inverval
    self.loop = loop  # loop real del main
    self.candle = candleBuilder("15min")
    self.df: Dict[str, pd.DataFrame] = {}
    self.time_frames = {"15 mins": "2 D"}
    self.ohlc = ["open", "high", "low", "close"]
    self.errors = [1100, 1101, 2103, 2105, 2110, 2157, 10182, 1102]
  
  # ------------------------------------------------------ LIFECYCLE ------------------------------------------------------ #
  def nextValidId(self, orderId):
    super().nextValidId(orderId)
    if not self.initialSetupDone:
      self.host_connected()
      self.initialSetupDone = True

  def host_connected(self):
    """Solicitar datos históricos al conectarse."""
    for durationString, time_frame in self.time_frames.items():
      self.reqHistoricalData(self.reqId, contract, "", time_frame, durationString,  "MIDPOINT", 0, 1, False, [])

      self.data[self.reqId] = {"symbol": contract.symbol + contract.currency,
                            "time_frame": durationString.replace(" ", ""), "bars": [],}

      self.reqTickByTickData(reqId=self.reqId , contract=contract, tickType="MidPoint", numberOfTicks=1, ignoreSize=False)

      self.reqId += 1

  # ---------------------------------- DATA HANDLING ---------------------------------- #
  def historicalData(self, reqId, bar):
    self.data[reqId]["bars"].append({
      "date": bar.date[:17],
      **{f: round(getattr(bar, f), 5) for f in self.ohlc}})
    
  def historicalDataEnd(self, reqId: int, start: str, end: str):
    bars = self.data[reqId]["bars"]
    symbol = self.data[reqId]["symbol"]
    time_frame = self.data[reqId]["time_frame"]
    key = f"{symbol}_{time_frame}".replace(" ", "").lower()

    self.df[key] = pd.DataFrame(bars)
    self.df[key]["date"] = pd.to_datetime(self.df[key]["date"])
    self.df[key].set_index("date", inplace=True)

    timestamp = self.df[key].index[-1]
    last_candle = self.df[key].iloc[-1].to_dict()

    self.candle.historical_update(timestamp, last_candle)

    asyncio.run_coroutine_threadsafe(historical_end(self.df[key], key), self.loop)

  def tickByTickMidPoint(self, reqId: int, time: int, midPoint: float):
    symbol = self.data[reqId]["symbol"]
    time_frame = self.data[reqId]["time_frame"]
    key = f"{symbol}_{time_frame}".replace(" ", "").lower()

    midPoint = round(midPoint, 5)
    self.candle.update(time, midPoint)

    candle = self.candle.get_candle()
    timestamp = self.candle.get_timestamp()

    asyncio.run_coroutine_threadsafe(update_candle(candle, time, key), self.loop)

  # ------------------------ ERROR HANDLING / RECONECTION ------------------------ #
  def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
    logging.error(f"Error {reqId} {errorCode} {errorString}")
    if errorCode in self.errors:
      self.reconnect()

  def reconnect(self):
    """Attempt to reconnect to the server."""
    if self.connState not in (EClient.CONNECTING,):
      self.disconnect()
      if hasattr(self, "_thread"):
        logging.info("Reconnecting to IB...")
        self.connect(host, port, clientid)
        self.run()

  def keepAlive(self):
    """Monitor and maintain the connection status."""
    data_lock = threading.Lock()

    while self.inverval:
      time.sleep(self.inverval)
      with data_lock:
        if not self.isConnected() and self.connState != EClient.CONNECTING:
          logging.error("Disconnected. Attempting to reconnect...")
          self.connect(host, port, clientid)
          self.run()      

  def disconnect(self):
    super().disconnect()
    self.initialSetupDone = False

# ------------------------------------ MAIN ------------------------------------ #
if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    app = IBapi(loop=loop)
    app.connect(host, port, clientid)

    # Hilos IB
    threading.Thread(target=app.run, daemon=True).start()
    threading.Thread(target=app.keepAlive, daemon=True).start()

    try:
      loop.run_forever()
    except KeyboardInterrupt:
      print("\n[INFO] Interrupción manual. Cerrando...")
    finally:
      app.disconnect()
      
      print("[INFO] Cliente IBAPI desconectado.")

      # Cerrar loop con limpieza mínima
      if not loop.is_closed():
        for task in asyncio.all_tasks(loop):
          task.cancel()

        loop.run_until_complete(asyncio.sleep(0))  # deja que se cancelen
        loop.close()

        print("[INFO] Loop asyncio cerrado.")
