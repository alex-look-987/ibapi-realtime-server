import pandas as pd

def equals(a, b, operator):
    return a >= b if operator == -1 else a <= b

def equals_inverted(a, b, operator):
    return a >= b if operator == 1 else a <= b

def greater_lower(a, b, operator):
    return a > b if operator == -1 else a < b

def greater_lower_inverted(a, b, operator):
    return a > b if operator == 1 else a < b

def compute(a, b, operator):
    return a - b if operator == -1 else a + b

def binary(df: pd.DataFrame, idx: int):
    column = 'high' if df.loc[idx, 'zz'] == -1 else 'low'

    return df.loc[idx, column]
