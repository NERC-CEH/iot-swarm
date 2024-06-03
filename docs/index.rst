.. IoT Thing Swarm documentation master file, created by
   sphinx-quickstart on Wed May 22 15:56:24 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. role:: python(code)
   :language: python

.. role:: shell(code)
   :language: shell

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   self
   source/modules
   genindex
   modindex

Welcome to IoT Thing Swarm's documentation!
===========================================

-----
About
-----

This package is a CLI tool for simulating a swarm of IoT devices for the FDRI project.
Currently it is capable of connecting to the COSMOS-UK database and mimicing a number
of sensor sites and trasmitting the most recent data as MQTT messages to IoT Core.
The code is designed to be modular and can be extended to other MQTT destinations or
randomised site and data production.

Because of the Global Intepreter Lock (GIL) in Python, this package is not truly
multithreaded, but uses :python:`async` methods to achieve a minimal amount of multithreaded behavior

------------------------------
Installation
------------------------------

Install this package via pip:

.. code-block:: shell

   pip install git+https://github.com/NERC-CEH/iot-swarm.git

This installs the package :python:`iotdevicesimulator` into your python environment.

-------------
Using The CLI
-------------

Installing this package will add a Command Line Interface (CLI) tool to your environment.
It can be invoked by typing :shell:`iot-swarm` into your terminal. The CLI can be called to
intialise a swarm with data sent every 30 minutes like so:

.. code-block:: shell

   iot-swarm cosmos --dsn="xxxxxx" --user="xxxxx" --password="*****" \
      mqtt "aws" LEVEL_1_SOILMET_30MIN "client_id" \
         --endpoint="xxxxxxx" \
         --cert-path="C:\path\..." \
         --key-path="C:\path\..." \
         --ca-cert-path="C:\path\..." \
         --sleep-time=1800 \
         --dry

Parameters such as the certificates, credentials, endpoints take up a lot of volume, and can be provided by environment variables instead:

.. code-block:: shell

   # COSMOS Credentials
   export IOT_SWARM_COSMOS_DSN="xxxxxxxx"
   export IOT_SWARM_COSMOS_USER="xxxxxxxx"
   export IOT_SWARM_COSMOS_PASSWORD="xxxxxxx"

   # AWS MQTT Configuration
   export IOT_SWARM_MQTT_ENDPOINT="xxxxxxxx"
   export IOT_SWARM_MQTT_CERT_PATH=="C:\path\..."
   export IOT_SWARM_MQTT_KEY_PATH="C:\path\..."
   export IOT_SWARM_MQTT_CA_CERT_PATH="C:\path\..."

Then the CLI can be called more cleanly:

.. code-block:: shell

   iot-swarm cosmos mqtt "aws" LEVEL_1_SOILMET_30MIN "client_id" --sleep-time=1800 --swarm-name="my-swarm"

------------------------
Using the Python Modules
------------------------

To create an IoT Swarm you must write a script such as the following:

.. code-block:: python

   from iotdevicesimulator.queries import CosmosQuery
   from iotdevicesimulator.swarm import CosmosSwarm
   from iotdevicesimulator.mqtt.aws import IotCoreMQTTConnection
   import asyncio
   import config
   from pathlib import Path
   import logging


   async def main(config_path: str):
      """Runs the main loop of the program.

      Args:
         config_path: A path to the config file for credentials and connection points.
      """
      log_config = Path(Path(__file__).parent, "__assets__", "loggers.ini")

      logging.config.fileConfig(fname=log_config)

      app_config = config.Config(config_path)

      iot_config = app_config["iot_core"]
      oracle_config = app_config["oracle"]

      mqtt_connection = IotCoreMQTTConnection(
         endpoint=iot_config["endpoint"],
         cert_path=iot_config["cert_path"],
         key_path=iot_config["key_path"],
         ca_cert_path=iot_config["ca_cert_path"],
         client_id="fdri_swarm",
      )
      swarm = await CosmosSwarm.create(
         CosmosQuery.LEVEL_1_SOILMET_30MIN,
         mqtt_connection,
         swarm_name="soilmet",
         delay_first_cycle=True,
         max_cycles=5,
         max_sites=5,
         sleep_time=30,
         credentials=oracle_config,
      )
      await swarm.run()


   if __name__ == "__main__":

      config_path = str(Path(Path(__file__).parent, "__assets__", "config.cfg"))
      asyncio.run(main(config_path))


This instantiates and runs a swarm of 5 sites from the COSMOS database that
each run for 5 cycles of queries and wait for 30 seconds between queries.

The system expects config credentials for the MQTT endpoint and the COSMOS Oracle database.

.. include:: example-config.cfg


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
