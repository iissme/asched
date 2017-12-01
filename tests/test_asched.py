from .test_asyncio_prepare import *
from functools import partial
from datetime import datetime, timedelta


@async_test
async def test_simple_task_without_sched(capfd):
    DelayedTask(loop, coro=partial(simple_task_1, 'simple_task_1'))\
        .at(datetime.now() + timedelta(seconds=1))

    await asyncio.sleep(1.5)
    out, err = capfd.readouterr()
    assert out.count('simple_task_1') == 1


@async_test
async def test_simple_task_sched(capfd):
    now = datetime.now()
    task_1 = sched.once(at=now + timedelta(seconds=2))
    await task_1.run(simple_task_1, 'simple_task_datetime')

    task_2 = sched.once().at(f'{now.hour}:{now.minute}:{now.second+1}')
    await task_2.run(simple_task_2, 'simple_task_str')

    await asyncio.sleep(3.5)
    out, err = capfd.readouterr()
    assert out.count('simple_task_datetime') == 1
    assert out.count('simple_task_str') == 1


@async_test
async def test_periodic_task(capfd):
    task = sched.every('2s', repeat=2)
    await task.run(periodic_task_1, 'periodic_task')

    await asyncio.sleep(1.5)
    assert sched.next_task_scheduled() == task

    await asyncio.sleep(3)
    out, err = capfd.readouterr()
    assert out.count('periodic_task') == 2


@async_test
async def test_periodic_task_at(capfd):
    now = datetime.now()
    task = sched.every('1s', repeat=2, start_at=now + timedelta(seconds=2))
    await task.run(periodic_task_1, 'periodic_task')

    await asyncio.sleep(1.8)
    assert task.done_times is 0

    await asyncio.sleep(3)
    out, err = capfd.readouterr()
    assert out.count('periodic_task') == 2


@async_test
async def test_failing_task(capfd):
    now = datetime.now()
    task = sched.every('1s', max_failures=2)
    await task.run(only_exception_task)

    await asyncio.sleep(2.5)
    assert task.failed_times == 0  # task done and reset


@async_test
async def test_multiple_periodic_tasks(capfd):
    task_1 = sched.every('2s', repeat=2)
    await task_1.run(periodic_task_1, 'periodic_task_1')

    task_2 = sched.every('1s', repeat=3)
    await task_2.run(periodic_task_2, 'periodic_task_2')

    await asyncio.sleep(4.5)
    out, err = capfd.readouterr()
    assert out.count('periodic_task_1') == 2
    assert out.count('periodic_task_2') == 3


@async_test
async def test_task_pause(capfd):
    task = sched.every('1s', repeat=3)
    await task.run(periodic_task_1, 'periodic_task')
    await asyncio.sleep(1.5)

    task.pause()
    assert task.done_times == 1
    await asyncio.sleep(1.5)

    task.resume()
    await asyncio.sleep(2.5)
    out, err = capfd.readouterr()
    assert out.count('periodic_task') == 3


sched = lrun_uc(AsyncShed(loop, conector=MongoConnector(db_name='test_db')))
lrun_uc(sched._db_connector.col.remove())
lrun_uc(asyncio.sleep(2))
loop.create_task(sched.start())

async def only_exception_task():
    raise Exception('Something is wrong!')

async def periodic_task_with_ex(i):
    if random.random() > 0.8:
        raise Exception('Periodic_task is broken')
    print(i)

async def simple_task_1(i):
    print(i)

async def simple_task_2(i):
    print(i)

async def periodic_task_1(i):
    print(i)

async def periodic_task_2(i):
    print(i)
