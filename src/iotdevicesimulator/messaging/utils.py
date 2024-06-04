from datetime import date, datetime


def json_serial(obj):
    """Serializes `obj` into a json object."""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat(timespec="microseconds")

    raise TypeError(f"Type {type(obj)} is not serializable.")
