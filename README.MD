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
`python -m iotswarm.livecosmos <config_src>`