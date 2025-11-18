import numpy as np
import pandas as pd
from itertools import product

''' functions_to_apply = {
    'mean':       False,
    'std':        False,
    'sum':        False,
    'max':        False,
    'min':        False,
    'median':     False,
    'skew':       False,
    'diff':       True,
    'pct_change': True} '''

agg_funcs = {
    'mean': lambda s, w: pd.Series(s).rolling(w).mean(),
    'std': lambda s, w: pd.Series(s).rolling(w).std(),
    'sum': lambda s, w: pd.Series(s).rolling(w).sum(),
    'max': lambda s, w: pd.Series(s).rolling(w).max(),
    'min': lambda s, w: pd.Series(s).rolling(w).min(),
    'median': lambda s, w: pd.Series(s).rolling(w).median(),
    'skew': lambda s, w: pd.Series(s).rolling(w).skew(),
    'diff': lambda s, w: pd.Series(s).diff(w-1),
    'pct_change': lambda s, w: pd.Series(s).pct_change(w)}

extractors_date = {
        'hour':      lambda d: pd.Series(d).dt.hour,     
        'minute':    lambda d: pd.Series(d).dt.minute,   
        'dayofweek': lambda d: pd.Series(d).dt.dayofweek,
        'month':     lambda d: pd.Series(d).dt.month,    
        'day':       lambda d: pd.Series(d).dt.day,      
        'week':      lambda d: pd.Series(d).dt.isocalendar().week.astype(int), 
        'year':       lambda d: pd.Series(d).dt.year}      

def computation(df: pd.DataFrame, functions_to_apply: dict, window: list, feature: list, class_: bool) -> pd.DataFrame:
    """
    Función para el procesamiento general de funciones a promedios móviles como:
    - mean
    - std
    - sum
    - max
    - min
    - median

    Args:
        df (pd.DataFrame): Dataframe con la información a procesar
        functions_to_apply (dict): Diccionario con valor booleano de las funcione
        window (list): Cantidad de diferentes ventanas: [3, 5, 8]
        feature (list): Total de valores a generar: ["high", "low"]
        class (bool): Clasificación de diff y pct_change: 1 subida, -1 bajada, 0 sin cambio

    Returns:
        pd.DataFrame: Dataframe con el siguiente tipo de columnas:
        - feature_func_window: high_mean_3
    """    
    
    active_funcs = {k: v for k, v in agg_funcs.items() if functions_to_apply.get(k, False)}

    for window, feature, func_name in product(window, feature, active_funcs):
        if feature in {'type'} and func_name in {'diff', 'pct_change'}:
            continue

        col_name = f'{feature}_{func_name}_{window}'
        df[col_name] = agg_funcs[func_name](df[feature], window)

        if class_ and func_name in {'diff', 'pct_change'}:
            df[f'{col_name}_class'] = np.where(df[col_name] > 0, 1, -1)

    return df

def date_feature(df: pd.DataFrame, dates: list[str]) -> pd.DataFrame:
    """
    Procesamiento de valores tipo Datetime como característica
    - minute
    - hour
    - day
    - dayofweek
    - week
    - month
    - year

    Args:
        df (pd.DataFrame): Dataframe con columna date
        dates (list[str]): valores datetime a generar

    Returns:
        pd.DataFrame: Dataframe con las columnas resultantes
    """    
    
    df['date'] = pd.to_datetime(df.index, format='mixed')

    for feature in dates:
        if feature in extractors_date:
            df[feature] = extractors_date[feature](df['date'])

    df.set_index('date', inplace=True)
    
    return df

def sessions(timestamp: pd.Timestamp):
    """
    Clasificación por zona horaria UTC de los mercados financierons

    Sesiones:
        - Sydney:     22:00 - 06:59
        - Tokyo:      00:00 - 08:59
        - London:     07:00 - 15:59
        - New York:   13:00 - 21:59

    Args:
        hour (_type_): _description_

    Returns:
        _type_: _description_
    """    

    hour = timestamp.hour
    minute = timestamp.minute
    time = hour + minute / 60

    if 22 <= time or time < 7:
        return 'sydney'
    elif 0 <= time < 9:
        return 'tokyo'
    elif 8 <= time < 17:
        if 13 <= time < 17:
            return 'overlap_london_ny'
        return 'london'
    elif 13 <= time < 22:
        return 'new_york'
    else:
        return 'none'
    
def session(df: pd.DataFrame):
    all_sessions = ['sydney', 'tokyo', 'london', 'overlap_london_ny', 'new_york']

    df['session'] = df.index.to_series().apply(sessions)
    
    dummies = pd.get_dummies(df['session'], dtype=int).reindex(columns=all_sessions, fill_value=0)
    
    df = pd.concat([df, dummies], axis=1)
    df.drop(columns='session', inplace=True)
    
    return df
