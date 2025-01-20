"""This module hold logic for device implementation. Currently only a single device time implemented."""

import asyncio
import enum
import logging
import random
from datetime import datetime
from typing import Any, List, TypedDict

from iotswarm import __version__ as package_version
from iotswarm.db import (
    BaseDatabase,
    CosmosDB,
    LoopingCsvDB,
    LoopingSQLite3,
    MockDB,
    Oracle,
)
from iotswarm.messaging.aws import IotCoreMQTTConnection
from iotswarm.messaging.core import MessagingBaseClass, MockMessageConnection
from iotswarm.queries import CosmosTable

logger = logging.getLogger(__name__)
logger.propagate = True


class BaseDevice:
    """Base class for sensing devices."""

    device_type: str = "base-device"
    """Name of the device type"""

    cycle: int = 0
    """Current cycle."""

    max_cycles: int = 0
    """Maximum number of data transfer cycles before shutting down."""

    sleep_time: int = 60
    """Time to sleep for each time data is sent."""

    device_id: str
    """ID of the site."""

    delay_start: bool = False
    """Adds a random delay to first invocation from 0 - `sleep_time`."""

    _instance_logger: logging.Logger
    """Logger used by the instance."""

    data_source: BaseDatabase
    """Specifies the source of data to use."""

    connection: MessagingBaseClass
    """Connection to the data receiver."""

    table: CosmosTable
    """SQL table used in queries if Oracle or LoopingSQLite3 selected as `data_source`."""

    mqtt_base_topic: str
    """Base topic for mqtt topic."""

    mqtt_prefix: str
    """Prefix added to mqtt message."""

    mqtt_suffix: str
    """Suffix added to mqtt message."""

    swarm: object | None = None
    """The session applied"""

    @property
    def mqtt_topic(self) -> str:
        "Builds the mqtt topic."
        topic = self._mqtt_topic
        if hasattr(self, "mqtt_prefix"):
            topic = f"{self.mqtt_prefix}/{topic}"

        if hasattr(self, "mqtt_suffix"):
            topic = f"{topic}/{self.mqtt_suffix}"

        return topic

    @mqtt_topic.setter
    def mqtt_topic(self, value: str) -> None:
        """Sets the mqtt topic"""
        self._mqtt_topic = value
        self.mqtt_base_topic = value

    _no_send_probability = 0

    @property
    def no_send_probability(self) -> int:
        """Defines the chance of data not being sent, can be 0 - 100"""
        return self._no_send_probability

    @no_send_probability.setter
    def no_send_probability(self, probability: int) -> None:
        """Setter for the no_send_probability attribute"""

        if not isinstance(probability, int):
            probability = round(probability)

        if probability < 0 or probability > 100:
            raise ValueError(f"`probability` must be between 0 - 100 inclusive, not '{probability}'")

        self._no_send_probability = probability

    def __eq__(self, obj: object) -> bool:
        if not isinstance(obj, BaseDevice):
            raise NotImplementedError

        base_equality = (
            self.device_type == obj.device_type
            and self.cycle == obj.cycle
            and self.max_cycles == obj.max_cycles
            and self.sleep_time == obj.sleep_time
            and self.device_id == obj.device_id
            and self.delay_start == obj.delay_start
            and self._instance_logger == obj._instance_logger
            and self.data_source == obj.data_source
            and self.connection == obj.connection
            and self.no_send_probability == obj.no_send_probability
        )

        table_equality = True
        if hasattr(self, "table") and not self.table == obj.table:
            table_equality = False

        mqtt_equality = True
        if hasattr(self, "mqtt_topic") and not self.mqtt_topic == obj.mqtt_topic:
            mqtt_equality = False

        return base_equality and table_equality and mqtt_equality

    def __init__(
        self,
        device_id: str,
        data_source: BaseDatabase,
        connection: MessagingBaseClass,
        *,
        sleep_time: int | None = None,
        max_cycles: int | None = None,
        delay_start: bool | None = None,
        inherit_logger: logging.Logger | None = None,
        table: CosmosTable | None = None,
        mqtt_topic: str | None = None,
        mqtt_prefix: str | None = None,
        mqtt_suffix: str | None = None,
        no_send_probability: int = 0,
    ) -> None:
        """Initializer

        Args:
            device_id: ID of site.
            data_source: Source of data to retrieve
            connection: Connection used to send messages.
            sleep_time: Time to sleep between requests (seconds).
            max_cycles: Maximum number of cycles before shutdown.
            delay_start: Adds a random delay to first invocation from 0 - `sleep_time`.
            inherit_logger: Override for the module logger.
            table: A valid table from the database
            mqtt_prefix: Prefixes the MQTT topic if MQTT messaging used.
            mqtt_suffix: Suffixes the MQTT topic if MQTT messaging used.
            no_send_probability: Defines the probability of the device not sending a message.
        """

        self.device_id = str(device_id)

        if not isinstance(data_source, BaseDatabase):
            raise TypeError(f"`data_source` must be a `BaseDatabase`. Received: {data_source}.")
        if isinstance(data_source, (CosmosDB)):
            if table is None:
                raise ValueError("`table` must be provided if `data_source` is type `OracleDB`.")
            elif not isinstance(table, CosmosTable):
                raise TypeError(f'table must be a "{CosmosTable.__class__}", not "{type(table)}"')

            self.table = table
        self.data_source = data_source

        if not isinstance(connection, MessagingBaseClass):
            raise TypeError(f"`connection` must be a `MessagingBaseClass`. Received: {connection}.")
        self.connection = connection

        if max_cycles is not None:
            max_cycles = int(max_cycles)
            if max_cycles < 0:
                raise ValueError(f"`max_cycles` must be 1 or more, or 0 for no maximum. Received: {max_cycles}")
            self.max_cycles = max_cycles

        if sleep_time is not None:
            sleep_time = int(sleep_time)
            if sleep_time < 0:
                raise ValueError(f"`sleep_time` must 0 or more. Received: {sleep_time}")
            self.sleep_time = sleep_time

        if delay_start is not None:
            if not isinstance(delay_start, bool):
                raise TypeError(f"`delay_start` must be a bool. Received: {type(delay_start)}.")
            self.delay_start = delay_start

        if isinstance(connection, (IotCoreMQTTConnection, MockMessageConnection)):
            if mqtt_topic is not None:
                self.mqtt_topic = str(mqtt_topic)
            else:
                self.mqtt_topic = f"{self.device_id}"

            if mqtt_prefix is not None:
                self.mqtt_prefix = str(mqtt_prefix)
            if mqtt_suffix is not None:
                self.mqtt_suffix = str(mqtt_suffix)

        if inherit_logger is not None:
            self._instance_logger = inherit_logger.getChild(f"{self.__class__.__name__}-{self.device_id}")
        else:
            self._instance_logger = logger.getChild(f"{self.__class__.__name__}-{self.device_id}")

        self.no_send_probability = no_send_probability

        self._instance_logger.info(f"Initialised Site: {repr(self)}")

    def __repr__(self):
        sleep_time_arg = f", sleep_time={self.sleep_time}" if self.sleep_time != self.__class__.sleep_time else ""
        max_cycles_arg = f", max_cycles={self.max_cycles}" if self.max_cycles != self.__class__.max_cycles else ""
        delay_start_arg = f", delay_start={self.delay_start}" if self.delay_start != self.__class__.delay_start else ""
        table_arg = (
            f", table={self.table.__class__.__name__}.{self.table.name}"
            if isinstance(self.data_source, CosmosDB)
            else ""
        )

        mqtt_topic_arg = (
            f', mqtt_topic="{self.mqtt_base_topic}"'
            if hasattr(self, "mqtt_base_topic") and self.mqtt_base_topic != self.device_id
            else ""
        )

        mqtt_prefix_arg = f', mqtt_prefix="{self.mqtt_prefix}"' if hasattr(self, "mqtt_prefix") else ""

        mqtt_suffix_arg = f', mqtt_suffix="{self.mqtt_suffix}"' if hasattr(self, "mqtt_suffix") else ""

        no_send_probability_arg = (
            f", no_send_probability={self.no_send_probability}"
            if self.no_send_probability != self.__class__._no_send_probability
            else ""
        )
        return (
            f"{self.__class__.__name__}("
            f'"{self.device_id}"'
            f", {self.data_source}"
            f", {self.connection}"
            f"{sleep_time_arg}"
            f"{max_cycles_arg}"
            f"{delay_start_arg}"
            f"{table_arg}"
            f"{mqtt_topic_arg}"
            f"{mqtt_prefix_arg}"
            f"{mqtt_suffix_arg}"
            f"{no_send_probability_arg}"
            f")"
        )

    async def _add_delay(self) -> None:
        delay = random.randint(0, self.sleep_time)
        self._instance_logger.debug(f"Delaying first cycle for: {delay}s.")
        await asyncio.sleep(delay)

    def _send_payload(self, payload: dict) -> bool:
        """Forwards the payload submission request to the connection

        Args:
            payload: The data to send.
        Returns:
            bool: True if sent sucessfully, else false.
        """

        if isinstance(self.connection, IotCoreMQTTConnection):
            return self.connection.send_message(payload, topic=self.mqtt_topic)
        else:
            return self.connection.send_message(payload)

    async def run(self) -> None:
        """The main invocation of the method. Expects a Oracle object to do work on
        and a table to retrieve. Runs asynchronously until `max_cycles` is reached.

        Args:
            message_connection: The message object to send data through
        """

        if self.delay_start:
            await self._add_delay()

        while True:
            if self.max_cycles > 0 and self.cycle >= self.max_cycles:
                break

            payload = await self._get_payload()

            if self._skip_send():
                self._instance_logger.debug(f"Skipped send based on probability: {self.no_send_probability}")
            elif payload is not None:
                payload = self._format_payload(payload)

                self._instance_logger.debug("Requesting payload submission.")

                send_status = self._send_payload(payload)

                if send_status:
                    self._instance_logger.info(
                        f"Message sent{f' to topic: {self.mqtt_topic}' if self.mqtt_topic else ''}"
                    )
                    self.cycle += 1

                    if isinstance(self.data_source, (LoopingCsvDB, LoopingSQLite3, MockDB)):
                        if self.swarm is not None:
                            self.swarm.write_self(replace=True)
            else:
                self._instance_logger.warning("No data found.")

            await asyncio.sleep(self.sleep_time)

    async def _get_payload(self) -> dict:
        """Method for grabbing the payload to send"""
        if isinstance(self.data_source, Oracle):
            return await self.data_source.query_latest_from_site(self.device_id, self.table)
        elif isinstance(self.data_source, LoopingSQLite3):
            return self.data_source.query_latest_from_site(self.device_id, self.table, self.cycle)
        elif isinstance(self.data_source, LoopingCsvDB):
            return self.data_source.query_latest_from_site(self.device_id, self.cycle)
        elif isinstance(self.data_source, BaseDatabase):
            return self.data_source.query_latest_from_site()

    def _format_payload(self, payload: dict) -> dict:
        """Oranises payload into correct structure."""
        return payload

    def _attach_swarm(self, swarm: object) -> None:
        self.swarm = swarm

    def _skip_send(self) -> bool:
        """Checks if the sending should be skipped

        Returns: True or false based on the no_send_probability
        """

        return random.random() * 100 < self.no_send_probability


class XMLDataTypes(enum.Enum):
    """Enum class representing XML datatypes with rankings used for selecting
    maximum type needed for a range of values."""

    null = {"schema": "xsi:nil", "rank": 0}
    string = {"schema": "xsd:string", "rank": 1}
    boolean = {"schema": "xsd:boolean", "rank": 2}
    dateTime = {"schema": "xsd:dateTime", "rank": 3}

    short = {"schema": "xsd:short", "rank": 4}
    int = {"schema": "xsd:int", "rank": 5}
    long = {"schema": "xsd:long", "rank": 6}
    integer = {"schema": "xsd:integer", "rank": 7}

    float = {"schema": "xsd:float", "rank": 8}
    double = {"schema": "xsd:double", "rank": 9}

    def __str__(self):
        return self.value["schema"]


class CR1000XField:
    """Represents the field part of a CR1000X payload. Each sensor gets a field."""

    name: str = ""
    """Name of the field."""

    data_type: str
    """XML datatype of the field."""

    units: str = ""
    """Scientific units of the field."""

    process: str = "Smp"
    """Process used for aggregating the data. Defaults to \"Smp\" meaning \"Sample\"."""

    settable: bool = False

    def __init__(
        self,
        name: str,
        data_type: str | None = None,
        units: str | None = None,
        process: str | None = None,
        settable: bool | None = None,
        data_values: list[any] | None = None,
    ):
        """Intializes the instance.

        Args:
            name: Name of the field variable.
            data_type: XML type of the data. `data_type` or `data_values` must
            be provided.
            units: Scientific unit of the measurement.
            process: Process used for data aggregation.
            settable: Defines whether the sensor measurment can be set.
            data_values: Used to calculate XML data type. Can be supplied
            instead of `data_type`. `data_type` or `data_values` must
            be provided.
        """

        self.name = str(name)

        if data_type is not None:
            self.data_type = str(data_type)
        elif data_values is not None:
            self.data_type = self._get_xsd_type(data_values).value["schema"]
        else:
            raise ValueError("`data_type` or `data_values` argument must be given.")

        if units is not None:
            self.units = str(units)

        if process is not None:
            self.process = str(process)
        else:
            self.process = CR1000XField._get_process(name)

        if settable is not None:
            if not isinstance(settable, bool):
                raise TypeError(f"`settable` argument must be a `bool`, not: `{type(settable)}`.")
            self.settable = settable

    def __json__(self):
        """Custom dunder method for converting instance to JSON representation."""

        return {
            "name": self.name,
            "type": self.data_type,
            "units": self.units,
            "process": self.process,
            "settable": self.settable,
        }

    def __repr__(self):
        settable_arg = f", settable={self.settable}" if self.settable != self.__class__.settable else ""
        process_arg = f', process="{self.process}' if self.process != self.__class__.process else ""
        units_arg = f', process="{self.units}' if self.units != self.__class__.units else ""
        return (
            f'{self.__class__.__name__}("{self.name}"'
            f', data_type="{self.data_type}"'
            f"{units_arg}"
            f"{process_arg}"
            f"{settable_arg}"
            f")"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CR1000XField):
            raise NotImplementedError

        return (
            self.name == other.name
            and self.data_type == other.data_type
            and self.units == other.units
            and self.process == other.process
            and self.settable == other.settable
        )

    @staticmethod
    def _get_avg_xsd_type(values: list) -> XMLDataTypes:
        """Calculates the most likely XML data type from a list of values.

        Args:
            values: The list of values to assess.
        Returns: The resultant type."""

        if not hasattr(values, "__iter__"):
            values = [values]

        highest = XMLDataTypes.null
        for item in values:
            level = CR1000XField._get_xsd_type(item)

            if level.value["rank"] > highest.value["rank"]:
                highest = level

        return highest

    @staticmethod
    def _get_xsd_type(value: str | int | float | bool | object) -> XMLDataTypes:
        """Converts a value to the XML data type expected.
        Args:
            value: The item to convert.

        Returns: The XML datatype.
        """
        if value is None:
            return XMLDataTypes.null

        if isinstance(value, datetime):
            return XMLDataTypes.dateTime

        if isinstance(value, bool):
            return XMLDataTypes.boolean

        if isinstance(value, str):
            try:
                datetime.fromisoformat(value)
                return XMLDataTypes.dateTime
            except ValueError:
                pass

            return XMLDataTypes.string

        if isinstance(value, int):
            if (value > 0 and value <= 32767) or (value < 0 and value >= -32768):
                return XMLDataTypes.short
            if value == 0 or (value < 0 and value >= -2147483648) or (value > 0 and value <= 2147483647):
                return XMLDataTypes.int
            if (value > 0 and value <= 9223372036854775807) or (value < 0 and value >= -9223372036854775808):
                return XMLDataTypes.long
            else:
                return XMLDataTypes.integer

        if isinstance(value, float):
            if abs(value) > 0 and (abs(value) < 1.1754943508222875e-38 or abs(value) > 3.4028234663852886e38):
                return XMLDataTypes.double
            else:
                return XMLDataTypes.float

        if hasattr(value, "__iter__"):
            return CR1000XField._get_avg_xsd_type(value)

        raise TypeError(f"Couldnt find XML datatype for value `{value}` and type: `{type(value)}`.")

    @staticmethod
    def _get_process(value: str) -> str:
        """Calculates the process attribute based on the variable name.

        Args:
            value: The variable name to generate from.

        Returns: The value of the expected process used.
        """

        value = value.lower()

        if value.endswith("_std"):  # Standard Deviation
            return "Std"
        elif value.endswith("_avg"):  # Average
            return "Avg"
        elif value.endswith("_max"):  # Maximum
            return "Max"
        elif value.endswith("_min"):  # Minimum
            return "Min"
        elif value.endswith("_mom"):  # Moment
            return "Mom"
        elif value.endswith("_tot"):  # Totalize
            return "Tot"
        elif value.endswith("_cov"):  # Covariance
            return "Cov"

        return "Smp"  # Sample


class CR1000XEnvironment(TypedDict):
    station_name: str
    table_name: str
    model: str
    serial_no: str
    os_version: str
    prog_name: str


class CR1000XPayloadHead(TypedDict):
    transaction: int
    signature: int
    environment: CR1000XEnvironment
    fields: List[CR1000XField]


class CR1000XPayloadData(TypedDict):
    time: datetime
    vals: List[Any]


class CR1000XPayload(TypedDict):
    head: CR1000XPayloadHead
    data: List[CR1000XPayloadData]


class CR1000XDevice(BaseDevice):
    "Represents a CR1000X datalogger."

    device_type = "CR1000X"

    serial_number: str = "00000"
    """Serial number of the device instance."""

    os_version: str = f"{device_type}.Std.07.02"
    """Operating system installed on the device."""

    program_name: str = f"CPU:iotswarm-{package_version}.CR1X"
    """Name of logger program being run."""

    table_name: str = "default"
    """Name of table being submitted by logger."""

    def __init__(
        self,
        *args,
        serial_number: str | None = None,
        os_version: str | None = None,
        program_name: str | None = None,
        table_name: str | None = None,
        **kwargs,
    ):
        """Initialises the class.

        Args:
            serial_number: Serial number of the device instance.
            os_version: Version of operating system used by device.
            program_name: Name of the program running on the datalogger.
            table_name: Name of the datalogger table being sent from datalogger.
        """

        super().__init__(*args, **kwargs)

        if serial_number is not None:
            self.serial_number = str(serial_number)
        else:
            self.serial_number = self._get_serial_number_from_site(self.device_id)

        if os_version is not None:
            self.os_version = str(os_version)

        if program_name is not None:
            self.program_name = str(program_name)

        if table_name is not None:
            self.table_name = str(table_name)

    @staticmethod
    def _steralize_payload(values: List[object] | object) -> List[dict]:
        """Converts an object or list of objects into a list of payloads."""

        if isinstance(values, dict):
            return [values]

        if not hasattr(values, "__iter__"):
            values = [values]

        if all([isinstance(x, dict) for x in values]):
            return values

        if any(not hasattr(v, "__iter__") for v in values):
            values = [values]

        values = [[v] if not hasattr(v, "__iter__") else v for v in values]

        keys = [f"_{i}" for i in range(len(values[0]))]

        dict_list = []
        for row in values:
            dict_list.append({k: v for k, v in zip(keys, row)})

        return dict_list

    def _format_payload(self, payload: dict) -> CR1000XPayload:
        """Formats the payload into datalogger method. Currently only suppports
        a single row of data.

        Args:
            payload: The payload object to format.

        Returns:
            dict: A dictionary of the formatted data.
        """

        f_payload = dict()

        f_payload["head"] = {
            "transaction": 0,
            "signature": 111111,
            "environment": {
                "station_name": self.device_id,
                "table_name": self.table_name,
                "model": self.device_type,
                "serial_no": self.serial_number,
                "os_version": self.os_version,
                "prog_name": self.program_name,
            },
        }

        payload = self._steralize_payload(payload)

        if len(set([len(p) for p in payload])) > 1:
            raise ValueError("Each payload row must be equal in length.")

        collected = dict()
        for i, row in enumerate(payload):
            for key in row.keys():
                if key not in collected:
                    collected[key] = [row[key]]
                    continue
                collected[key].append(row[key])

        time = None
        for i, k in enumerate(collected.keys()):
            if k.lower() == "date_time":
                time = collected.pop(k)
                break

        if time is None:
            time = [datetime.now().isoformat()] * len(payload)

        f_payload["data"] = []
        keys = list(collected.keys())
        vals = list(collected.values())
        for i in range(len(payload)):
            f_payload["data"].append({"time": time[i], "vals": [x[i] for x in vals]})

        f_payload["head"]["fields"] = [CR1000XField(k, data_values=v) for k, v in zip(keys, vals)]

        return f_payload

    @staticmethod
    def _get_serial_number_from_site(value: str) -> str:
        """Generates a serial number from a string value.
        Converts the characters into dash separated numbers.

        Args:
            value: The string value to generate the id from.

        Returns: A string serial number"""

        value = str(value)

        return "-".join([str(ord(x)) for x in value])
