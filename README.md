# asched

Python 3.6+

asched can schedule your asyncio coroutines for a specific time or interval, keep their run stats and reschedule them from the last state when restarted.

## About
asched currenly uses [asyncio-mongo-reflection][amr_link] (powered by [mongodb][mongodb_link]) to store data.

## Install
Only manual install via setup.py currently ([asyncio-mongo-reflection][amr_link] has to be installed).

## Documentation
Work in progress.

## Dependencies
* [asyncio-mongo-reflection][amr_link]

[mongodb_link]: https://www.mongodb.com/
[amr_link]: https://github.com/isanich/asyncio-mongo-reflection