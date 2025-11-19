from itertools import product

# DELTA MODELS

DELTA_DIR = ['rgbp', 'rgbn', 'avrgp', 'avgrn', 'upper_shadow', 'lower_shadow',
              'bb_class', 'upper_tail', 'body', 'lower_tail', 'color', 'high_diff', 'low_diff',
              'pos_direction', 'neg_direction']

# COMPUTATION
# ----------------------------------------------------------------------------------------------------------------------------- #

dates, sessions = ['minute', 'hour', 'day', 'dayofweek', 'month'], ['tokyo', 'sydney', 'new_york', 'london']

window_delta, feature_delta = [3, 5, 8], ['close']

funcs_delta = {'mean': True} 
active_funcs_delta = {k: v for k, v in funcs_delta.items() if funcs_delta.get(k, False)}

computations_delta = [f'{feature}_{func}_{window_delta}' for window_delta, feature, func in product(window_delta, feature_delta, active_funcs_delta)]

DELTA_DIR = DELTA_DIR + computations_delta + dates[:-1]