"""Module for handling commonly reused utility functions."""

from datetime import date, datetime

def json_serial(obj: object):
    """Serializes an unknown object into a json format."""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat(timespec="microseconds")

    if obj.__class__.__module__ != "builtins":
        return obj.__json__()

    raise TypeError(f"Type {type(obj)} is not serializable.")