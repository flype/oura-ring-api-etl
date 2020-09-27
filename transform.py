"""
transform.py
"""

def transform_sleep_data(data):
    for row in data:
        # map the hypnogram data to a more readable format
        # 'D' = deep sleep
        # 'L' = light sleep
        # 'R' = REM sleep
        # 'A' = awake
        row['hypnogram_5min'] = ['DLRA'[int(c)-1] for c in row['hypnogram_5min']]
        row['is_longest'] = bool(row['is_longest'])

    return data


def transform_activity_data(data):
    for row in data:
        row['class_5min'] = [ int(c) for c in row['class_5min']]

    return data


def transform_readiness_data(data):
    return data
