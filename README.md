# asched

Python 3.6+

asched can schedule your asyncio coroutines for a specific time or interval, keep their run stats and reschedule them from the last state when restarted.

## Install
Clone from git and install via setup.py.
Or `pip install -U https://github.com/isanich/asched/archive/master.zip`.

## Documentation
Look at example below.
```py
import asyncio
from functools import partial
from datetime import datetime, timedelta
from asched import AsyncSched, MongoConnector, DelayedTask


async def echo(i):
    print(i)


async def main():
    # Currently asched keeps information about running tasks in single MongoDB array
    # which can be placed in the new or existing mongo object.
    # `db_name` is only a required argument for MongoConnector (leads to creation of `async_sched`
    # collection with default tasks object inside.
    connector = MongoConnector(db_name='test_db',
                               col_name='async_sched',  # new or existing collection
                               # if existing object by `obj_ref` is not found than new one is created
                               obj_ref={'tasks_id': 'example tasks object'},
                               # `key` where to store tasks array
                               # could be smth. like `embed.embed.embed.asched_tasks`
                               key='asched_tasks')

    # Scheduler instance is used for tasks creation
    sched = await AsyncSched(loop, conector=connector)

    # but in some cases you can create tasks without scheduler
    dt = DelayedTask(loop, coro=partial(echo, 'simple delayed task'))
    # `dt` will be fired only once and no data is passed to db
    dt.at(datetime.now() + timedelta(seconds=1))

    # Tasks made by scheduler are more common and could be automatically rescheduled
    # from the last state if you program is restarted (you should manually create the same task
    # actually but it will receive the same state as it had before restart).
    # `run` method is used to start task and pass reference to coroutine function and its arguments.
    forever_task = sched.every('1s')  # task can be created synchronously
    await forever_task.run(echo, 'every 1s')  # but could be started only with `await`
    await asyncio.sleep(2)  # let task actually be fired

    # `await forever_task.pause()` task can be paused
    # `await forever_task.resume()` resumed
    await forever_task.cancel()  # or canceled

    # `every` can accept multiple arguments. first one is `iterval` and it's required.
    # `iterval` could be passed as timecode string in the following format: `1mo2w3d5h10m20s`
    # (1 month 2 weeks 3 days 5 hours 10 minutes 20 seconds) or as a simple integer (seconds).
    # Note that tasks without `run` won't be fired.

    await sched.every('1s', repeat=3).run(echo, 'every 1s repeat 3')
    # `repeat` - how many times task should be repeated.

    start_at = datetime.now() + timedelta(seconds=2)
    await sched.every('1s', repeat=2, start_at=start_at).run(echo, f'every 1s repeat 2 at {start_at}')
    # `start_at` - when the first run should be scheduled.

    sched.every('1s', max_failures=3)  # maximum fails with exception before task is auto canceled
    sched.every('1s', max_failures=3, exc_type=TypeError)  # exception type can be specified
    # Note that asched has builtin logging so just provide your own logger
    # to watch for any exception information.

    # You can schedule task once with `once` method and `at` argument that could be passed as `str`
    # in format hh:mm:ss
    now = datetime.now()
    once_str = f'{now.hour}:{now.minute}:{now.second+2 if now.second <= 58 else 2}'
    await sched.once(at=once_str).run(echo, f'once at {once_str}')

    # or Datetime
    later = datetime.now() + timedelta(seconds=3)
    await sched.once(at=later).run(echo, f'once at {later}')
    # `sched.once().at(later)` you could also use `at` as method

    sched.next_task_scheduled()  # returns nextly scheduled task
    final_task = await sched.every('1s', repeat=1).run(echo, 'final every 1s repeat 1')
    await asyncio.sleep(10)  # let all tasks finish

    # when task is finished its stats are reset so you can schedule it once more with 'add_task'
    await sched.add_task(final_task)
    await asyncio.sleep(2)  # let it run too

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
```

## Dependencies
asched currenly uses [asyncio-mongo-reflection][amr_link] (powered by [mongodb][mongodb_link]) to store data.

[mongodb_link]: https://www.mongodb.com/
[amr_link]: https://github.com/isanich/asyncio-mongo-reflection
