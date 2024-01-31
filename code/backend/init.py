

engine_set = ["Spark", "PostgreSQL", "ClickHouse"]
recommend_method_set = ["Greedy", "RL"]
single_or_multiple_set = ["Single", "Multiple"]

system_start = False
system_stage = 0
last_system_stage = 0
log_msg = 'log'


def check_valueisInt(value, value_name):
    if not value:
        return f"{value_name} is None!"
    if not isinstance(value, int):
        return f"{value_name} must be int!"
    return None