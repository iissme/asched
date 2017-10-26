from asched import *
import asyncio
import pytest
import logging
import os

loop = asyncio.get_event_loop_policy().new_event_loop()
asyncio.set_event_loop(loop)
loop.set_debug(True)
loop._close = loop.close
loop.close = lambda: None

run = lambda coro: loop.run_until_complete(coro)

tests_root = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(tests_root, 'tests.log')

if os.path.isfile(log_file):
    os.unlink(log_file)

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s\n',
                    datefmt='%H:%M:%S',
                    filename=log_file,
                    filemode='w')


@pytest.fixture(scope="session", autouse=True)
def test_shutdown():
    yield None
    # clear all tasks before loop stops
    pending = asyncio.Task.all_tasks()
    for task in pending:
        task.cancel()

    run(asyncio.sleep(1))
    loop.close()
