"""
asched
~~~~~~~~~~~~~~~~~~~
Schedule your asyncio coroutines for a specific time or interval, keep their run stats and reschedule them from the last state when restarted.
:copyright: (c) 2017 isanich
:license: MIT, see LICENSE for more details.
"""
import logging
from .asched import (AsyncShed, DelayedTask, MongoConnector,
                     DelayedTaskExeption, WrongTimeDataExeption, ShedulerExeption)

logging.getLogger(__name__).addHandler(logging.NullHandler())