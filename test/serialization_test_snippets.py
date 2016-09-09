import numpy as np
import pandas as pd

serialization_test_obj = {
  'infinity': np.inf,
  'Inf': np.Inf,
  'Infinity': np.Infinity,
  'None': None,
  'NaN': np.nan,
  'NaT': pd.NaT,
  'np.ndarray': np.ndarray(3),
  'pd.Series': pd.Series([1,2,3]),
  'pd.DataFrame': pd.DataFrame({'a': [1, 2, 3]}),
  'pd.DataFrameWithInvalid': pd.DataFrame({'a': [np.nan, np.Infinity, 3]}),
}

from math import isnan, isinf

def replace_unserializable_numpy(obj):
    if isinstance(obj, dict):
        return dict((k, replace_unserializable_numpy(v)) for k, v in obj.items())
    elif isinstance(obj, np.float32) or isinstance(obj, np.float64):
        if isnan(obj) or isinf(obj):
            return None
        return obj.item()
    elif isinstance(obj, float) or isinstance(obj, int):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return obj
    elif isinstance(obj, (np.ndarray, list, tuple)):
        return map(replace_unserializable_numpy, obj)
    elif isinstance(obj, (pd.DataFrame, pd.Series)):
        return replace_unserializable_numpy(obj.to_dict())
    elif obj == None:
        return None
    elif isinstance(obj, str) or isinstance(obj, unicode) or isinstance(obj.keys()[0], unicode):
        return obj.replace('nan', 'null').replace('NaN', 'null')
    else:
        return obj


  %timeit replace_unserializable_numpy(serialization_test_obj)
  %timeit make_serializable(serialization_test_obj)
