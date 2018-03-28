"""
asched
~~~~~~~~~~~~~~~~~~~
Schedule your asyncio coroutines for a specific time or interval, keep their run stats and reschedule them from the last state when restarted.
:copyright: (c) 2018 isanich
:license: MIT, see LICENSE for more details.
"""
import logging
from .asched import (AsyncSched, DelayedTask, MongoConnector,
                     DelayedTaskExeption, WrongTimeDataException, ShedulerExeption)

logging.getLogger(__name__).addHandler(logging.NullHandler())
