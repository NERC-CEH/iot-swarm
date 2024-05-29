import autosemver

try:
    __version__ = autosemver.packaging.get_current_version(
        project_name="iot-device-simulator"
    )
except:
    __version__ = "unknown version"
