# IoT Thing Swarm
[![tests badge](https://github.com/NERC-CEH/iot-swarm/actions/workflows/test.yml/badge.svg)](https://github.com/NERC-CEH/iot-swarm/actions)
[![docs badge](https://github.com/NERC-CEH/iot-swarm/actions/workflows/doc-deployment.yml/badge.svg)](https://nerc-ceh.github.io/iot-swarm/)

This is a Python package intended to simulate a swarm of IoT device communications via MQTT, enabling stress testing of cloud infrastructure with loads close to production level. [Read the docs](https://nerc-ceh.github.io/iot-swarm/)

# Live Cosmos Data

To use the live cosmos data tool you must create a config file in the format:

```yaml
dsn='<dsn>'
user='<username>'
pass='<password>'
```
The module script can then be triggered by running the module with an argument:
`python -m iotswarm.livecosmos <config_src> <cosmos_table>`

## How it Works
When the main module is triggered, it received the oracle credentials path and COSMOS table via command line arguments. This then creates an "LiveUploader" object which works with a state object to know which what data line was the most recently uploaded. It then requests all data after that point, creates payload objects, and uploads them to S3. Currently it just pretends to upload them and update the state.

## The State object

The state object takes in a COSMOS table name and manages a state object / file with it. The file will be stored in `~/.local/state/livecosmos/<filename>.pickle`, with a backup in the same folder. When a state object is initiated, it does the following:
1. Attempts to load the state from file
2. If the state file doesn't exist or is corrupted, it moved onto the backup file
3. If the backup file exists, it is loaded and the main file overwritten by the backup
4. If the backup is also corrupted, the program exits
5. If neither file exists, a new empty state is created

When a new upload is added to the state, the dictionary `{"site_id": "<site_id>", "last_data": <time_object>}` is added to the state if the site_id does not exist or if the "last_data" is more recent than currently existing in the state.

In the LiveUploader, when a file uploads sucessfully, it tells the state to write to file.