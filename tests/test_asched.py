from .test_asyncio_prepare import *
from functools import partial
from datetime import datetime, timedelta


def test_simple_task_without_sched(capfd):
    DelayedTask(loop, coro=partial(simple_task_1, 'simple_task_1'))\
        .at(datetime.now() + timedelta(seconds=1))

    run(asyncio.sleep(1.5))
    out, err = capfd.readouterr()
    assert out.count('simple_task_1') == 1


def test_simple_task_sched(capfd):
    now = datetime.now()
    task_1 = sched.once().at(now + timedelta(seconds=2))
    task_1.run(simple_task_1, 'simple_task_datetime')

    task_2 = sched.once().at(f'{now.hour}:{now.minute}:{now.second+1}')
    task_2.run(simple_task_2, 'simple_task_str')

    run(asyncio.sleep(3.5))
    out, err = capfd.readouterr()
    assert out.count('simple_task_datetime') == 1
    assert out.count('simple_task_str') == 1


def test_periodic_task(capfd):
    task = sched.every('2s', repeat=2)
    task.run(periodic_task_1, 'periodic_task')

    run(asyncio.sleep(4.5))
    out, err = capfd.readouterr()
    assert out.count('periodic_task') == 2


def test_failing_task(capfd):
    task = sched.every('1s', max_failures=2)
    task.run(only_exception_task)

    run(asyncio.sleep(2.5))
    assert task.failed_times == 0  # task done and reset


def test_multiple_periodic_tasks(capfd):
    task_1 = sched.every('2s', repeat=2)
    task_1.run(periodic_task_1, 'periodic_task_1')

    task_2 = sched.every('1s', repeat=3)
    task_2.run(periodic_task_2, 'periodic_task_2')

    run(asyncio.sleep(1.5))
    assert sched.next_task_scheduled() == task_1

    run(asyncio.sleep(3))
    out, err = capfd.readouterr()
    assert out.count('periodic_task_1') == 2
    assert out.count('periodic_task_2') == 3


def test_task_pause(capfd):
    task = sched.every('1s', repeat=3)
    task.run(periodic_task_1, 'periodic_task')
    run(asyncio.sleep(1.5))

    task.pause()
    assert task.done_times == 1
    run(asyncio.sleep(1.5))

    task.resume()
    run(asyncio.sleep(2.5))
    out, err = capfd.readouterr()
    assert out.count('periodic_task') == 3


sched = AsyncShed(loop, conector=MongoConnector.create(db_name='test_db'))
run(sched._db_connector.col.remove())
run(asyncio.sleep(2))
loop.create_task(sched.run())
# loop.create_task(sched.run())
# task0 = sched.every('2s', repeat=5, max_failures=5)
# task0.run(periodic_task, 555)
# loop.run_until_complete(asyncio.sleep(6))
# task0.run(repeat22, 666)
# task1 = sched.every('2s', max_failures=2).run(periodic_task, 11)
# task2 = sched.every('2s', repeat=2).run(repeat22, 22)
# task4 = sched.once().at('20:34').run(repeat22, 22222222)
# task5 = sched.once().at(datetime(year=2017, month=5, day=24, hour=20)).run(test_res, 44)
# task3 = sched.every('2s', repeat=5).run(repeat42, 33)

# task3.pause()

# loop.run_until_complete(asyncio.sleep(16))
# task3.resume()
# loop.run_until_complete(sched.add_task(task2.run(repeat22, 666)))
# sched.next_run()

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
