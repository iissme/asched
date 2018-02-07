from asched import *
import asyncio
import pytest
import logging
import os

from functools import wraps

loop = asyncio.get_event_loop_policy().new_event_loop()
asyncio.set_event_loop(loop)
loop.set_debug(True)
loop._close = loop.close
loop.close = lambda: None

lrun_uc = loop.run_until_complete

tests_root = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(tests_root, 'tests.log')

if os.path.isfile(log_file):
    os.unlink(log_file)

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s\n',
                    datefmt='%H:%M:%S',
                    filename=log_file,
                    filemode='w')


log = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def test_shutdown():
    yield None
    # clear all tasks before loop stops
    pending = asyncio.Task.all_tasks()
    for task in pending:
        task.cancel()

    lrun_uc(asyncio.sleep(1))
    loop.close()


def async_test(testf):
    @wraps(testf)
    def tmp(*args, **kwargs):
        log.info(f'Starting test - {tmp.__name__}...')
        res = lrun_uc(testf(*args, **kwargs))
        log.info(f'Test - {tmp.__name__} is finished!')
        return res
    return tmp
