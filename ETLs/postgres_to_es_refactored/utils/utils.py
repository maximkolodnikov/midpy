import datetime
from functools import wraps
from typing import Set


def coroutine(func):
    """Simple wrapper to make the generator function act like coroutine."""
    @wraps(func)
    def inner(*args, **kwargs):
        fn = func(*args, **kwargs)
        next(fn)
        return fn
    return inner


def default_json_encoder(o):
    """ Обрабатывает datetime и set для json. """
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()

    if isinstance(o, Set):
        return list(o)
