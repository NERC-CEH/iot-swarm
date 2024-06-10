from datetime import date, datetime


def json_serial(obj):
    """Serializes `obj` into a json object."""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat(timespec="microseconds")

    if obj.__class__.__module__ != "builtins":
        return obj.__json__()

    raise TypeError(f"Type {type(obj)} is not serializable.")
