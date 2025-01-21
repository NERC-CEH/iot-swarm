"""Module for handling commonly reused utility functions."""

from datetime import date, datetime
import json

def json_serial(obj: object) -> str:
    """Serializes an unknown object into a json format."""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat(timespec="microseconds")

    # if obj.__class__.__module__ != "builtins" and hasattr(obj, "__json__"):
    #     return obj.__json__()

    if obj.__class__.__module__ != "builtins":
        return obj.__json__()

    # if hasattr(obj, "__dict__"):
    #     return obj.__dict__()

    raise TypeError(f"Type {type(obj)} is not serializable.")
