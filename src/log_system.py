import pandas as pd
import asyncio, logging
from pathlib import Path
from datetime import datetime, timedelta
from trade_mgmt.manager import TradeManager

logging.basicConfig(level=logging.INFO, format="%(message)s")

class LogManager:
    def __init__(self):
        self.logTime = None
        self.lastLogHour = None
        self.logInterval = timedelta(hours=4)
        self.timeIntervals = [2, 6, 10, 14, 18, 22]
        self.sleepInterval = timedelta(minutes=30).total_seconds()
        
        base_folder = Path.cwd()
        self.folder = base_folder / "logs" 
        self.folder.mkdir(parents=True, exist_ok=True)

        self._lock = asyncio.Lock()

    # ------------------ CÁLCULO DE INTERVALOS ------------------ #
    def initialInterval(self):
        runTime = datetime.now().hour
    
        next_interval = next((i for i in self.timeIntervals if runTime < i), self.timeIntervals[0])
        self.logTime = datetime.strptime(f"{next_interval:02d}:00", "%H:%M").time()
        
        logging.info(f"Starting Server Time: {runTime} | Next Log Time: {self.logTime}")

    def nextInterval(self):
        new_time = datetime.combine(datetime.today(), self.logTime) + self.logInterval
        self.logTime = new_time.time()

        logging.info(f"Próxima ejecución programada: {self.logTime}")

    # ------------------ TAREA PRINCIPAL ------------------ #
    async def schedulerLogTask(self, monitoring: TradeManager):
        while True:
            now = datetime.now()

            # Si estamos en o después de la hora programada, y aún no se ha ejecutado en este ciclo
            if now.hour >= self.logTime.hour and self.lastLogHour != self.logTime.hour:
                if monitoring.db_trades:
                    await self.logTrades(monitoring, now)
                else:
                    logging.info("Sin trades activos o registrados; omitiendo log.")
                
                self.nextInterval()
                self.lastLogHour = self.logTime.hour

            await asyncio.sleep(self.sleepInterval)

    # ------------------ GUARDADO DE TRADES ------------------ #
    async def logTrades(self, monitoring: TradeManager, now: datetime):
        historical_trades = monitoring.historicalTrades(loggin=True)

        date = now.strftime("%Y%m%d_%H%M")
        filename = self.folder / f"operations_{date}.csv"

        try:
            await self.asyncSaveCsv(historical_trades, filename)
            logging.info(f"Trades guardados exitosamente [{date}] ({len(historical_trades)} registros)")
            
        except Exception as e:
            logging.exception(f"Error guardando trades: {e}")

    # ------------------ GUARDADO ASÍNCRONO ------------------ #
    async def asyncSaveCsv(self, df: pd.DataFrame, filename: Path):
        """Guarda CSV de forma segura usando lock asíncrono (sin archivo temporal)."""
        async with self._lock:
            try:
                await asyncio.to_thread(df.to_csv, filename, index=False, compression='gzip')
            except Exception as e:
                logging.exception(f"Error al guardar CSV: {e}")
                raise
