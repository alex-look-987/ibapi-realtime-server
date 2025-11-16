import numpy as np
import pandas as pd
from pathlib import Path
import asyncio, joblib, logging
from sklearn.ensemble import RandomForestClassifier

class ServerManager:
    """Encapsula todo el estado del servidor."""
    
    def __init__(self):
        self.lags = 4
        self.pip = 1e-4 # 0.0001 pip conversion 
        self.cost = 0.00004
        self.frames = [0, 0]
        self.historical_data = {}
        self.producer_clients = {}
        self.current_candle = None
        self.heartbeat_interval = 300
        self.historical_clients = set()
        self.end_event = asyncio.Event()

    # ---------------------- Heartbeat ---------------------- #
    async def heartbeat(self):  # cada 10 min
        while True:
            logging.info("Servidor activo y con conexión...")
            await asyncio.sleep(self.heartbeat_interval)

class ModelManager:
    """carga y gestión de modelos delta direction high/low"""
    
    key_map = {"high": "delta_high_model", "low": "delta_low_model"}

    def __init__(self, model_folder: str):
        self.models: dict = {}
        self.model_folder = Path(model_folder)

    def load_models_from_folder(self):
        """modelos pkl"""

        for model_file in self.model_folder.glob("*.pkl"):
            key = model_file.stem
            self.models[key] = joblib.load(model_file)

        logging.info(f"Modelos cargados: {list(self.models.keys())}")

    async def predict_async(self, key: str, features: pd.DataFrame):
        """Predicción asíncrona con tipado para VotingClassifier o VotingRegressor"""

        if key not in self.key_map:
            raise ValueError(f"Key '{key}' no soportada")

        model_key = self.key_map[key]
        model_entry = self.models.get(model_key)
        
        if model_entry is None:
            raise ValueError(f"Modelo '{model_key}' no cargado")

        model: RandomForestClassifier = model_entry['model']

        X = features[model.feature_names_in_]

        loop = asyncio.get_running_loop()
        prediction = await loop.run_in_executor(None, model.predict, X)

        return int(prediction[0])