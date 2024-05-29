.. IoT Thing Swarm documentation master file, created by
   sphinx-quickstart on Wed May 22 15:56:24 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

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

------------------------------
Installation and Configuration
------------------------------

Install this package via pip:

.. code-block:: shell

   pip install git+https://github.com/NERC-CEH/iot-swarm.git


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   self
   source/modules
   genindex
   modindex


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
