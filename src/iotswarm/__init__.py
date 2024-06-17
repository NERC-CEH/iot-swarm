import autosemver

try:
    __version__ = autosemver.packaging.get_current_version(project_name="iot-swarm")
except:
    __version__ = "unknown version"
