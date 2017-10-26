import asyncio
import hashlib
import logging
import random
import re
from collections import deque
from datetime import datetime, timedelta, time
from functools import partial

import motor.motor_asyncio

from asyncio_mongo_reflection import MongoDequeReflection

log = logging.getLogger(__name__)


class DelayedTaskExeption(Exception):
    pass


class WrongTimeDataExeption(Exception):
    pass


class ShedulerExeption(Exception):
    pass


class DelayedTask:

    class SyncProp:
        def __init__(self, name):
            self.name = name

        def __get__(self, instance, owner):
            return instance.__dict__[self.name]

        def __set__(self, instance, value):
            sync_name = self.name
            sync_value = value
            db_connector = instance._db_connector

            if 'hash' in instance.__dict__:  # after __init__
                if instance._db_connector is not None and db_connector.is_connected:
                    if self.name == '_coro':
                        sync_name = 'runs'
                        sync_value = instance._get_coro_desc(value)

                    async_run = instance._loop.create_task
                    instance.__dict__['_sync_prop_task'] = async_run(
                        db_connector._sync_prop(instance.hash, sync_name, sync_value))

            instance.__dict__[self.name] = value

    repeat = SyncProp('repeat')
    interval = SyncProp('interval')
    is_paused = SyncProp('is_paused')
    next_run_at = SyncProp('next_run_at')
    last_run_at = SyncProp('last_run_at')
    done_times = SyncProp('done_times')
    failed_times = SyncProp('failed_times')
    hash = SyncProp('hash')
    _coro = SyncProp('_coro')

    def __init__(self, loop, *,
                 db_connector=None, coro=None,
                 interval=None, repeat=None,
                 max_failures=None, supervisor=None):

        self._loop = loop
        self._db_connector = db_connector
        self._task_supervisor = supervisor
        self._sync_prop_task = None
        self._coro = coro
        self._future = asyncio.Future()

        self.repeat = repeat
        self.interval = interval
        self.max_failures = max_failures if max_failures else repeat
        self._set_default_stats()

        task_hash = hashlib.sha1(str(hash(self)).encode('utf-8'))
        self.hash = task_hash.hexdigest()[:10]

    def _set_default_stats(self):
        self._should_be_reshedulled = True if self.interval else False
        self._is_idle = True

        self.is_paused = False
        self.timer = None

        self.next_run_at = None
        if not self.is_done:
            self.last_run_at = None

        self.done_times = 0
        self.failed_times = 0

    def _callback(self):

        def future_wrapper(coro, future):
            async def inner():
                try:
                    if future.cancelled():
                        return

                    res = await coro()
                except Exception as e:
                    self.failed_times += 1
                    if self.max_failures and self.failed_times == self.max_failures:
                        self._should_be_reshedulled = False

                    future.set_exception(e)
                else:
                    self.done_times += 1
                    if self.repeat == self.done_times:
                        self._should_be_reshedulled = False

                    future.set_result(res)
                finally:
                    return future
            return inner

        safe_run = lambda c: asyncio.run_coroutine_threadsafe(c, self._loop)
        safe_run(future_wrapper(self._coro, self._future)())

    def _schedule(self):
        self._is_idle = False

        if self.is_done or self.is_cancelled:
            self._future = asyncio.Future()

        now = datetime.now()
        if self.interval:
            self.next_run_at = now + timedelta(seconds=self.interval)
        elif self.next_run_at < now:
            raise WrongTimeDataExeption('Passed date should be later then \'now\'!')

        delta = self.next_run_at - now
        loop_correction = getattr(self._task_supervisor, 'loop_resolution', 0)
        run_at = self._loop.time() + delta.total_seconds() - loop_correction

        self.timer = self._loop.call_at(run_at, self._callback)
        log.info(f'Task is sheduled!\n{self}')

    def _reshedule(self):
        self.last_run_at = self.next_run_at
        self._schedule()

    def cancel(self):
        self._future.cancel()
        self._set_default_stats()

    def pause(self):
        if self.is_paused or self.is_cancelled:
            raise DelayedTaskExeption('Only running tasks can be paused!')

        self.is_paused = True
        self._future.cancel()

    def resume(self):
        if not self.is_paused:
            raise DelayedTaskExeption('You can\'t resume task that is not paused!')

        self.is_paused = False
        self._schedule()

    def at(self, sched_date):

        if self.interval is not None:
            raise DelayedTaskExeption('Intervals don\'t work with \'at\' method!')

        if type(sched_date) is str:
            hh_mm_re = re.compile(r'^(?P<hours>\d{,2}):(?P<minutes>\d{,2}):(?P<seconds>\d{,2})$')
            match = re.match(hh_mm_re, sched_date)
            if match:
                hour = int(match.group('hours').lstrip('0'))
                minute = int(match.group('minutes').lstrip('0'))
                second = int(match.group('seconds').lstrip('0'))
                assert 0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59
                sched_date = datetime.combine(datetime.now(), time(hour, minute, second))
            else:
                raise WrongTimeDataExeption('hh:mm formated string is expected!')

        elif not isinstance(sched_date, datetime):
            raise WrongTimeDataExeption('Datetime or str instance is expected!')

        self.next_run_at = sched_date

        # allow task self invoke (use without supervisor)
        if self._coro:
            self._schedule()

        return self

    def run(self, coro, *args, **kwargs):
        if asyncio.iscoroutinefunction(coro):
            self._coro = partial(coro, *args, **kwargs)

            # make sure we've set coro desc in db before new hash
            self._loop.run_until_complete(self._sync_prop_task)

            str_task = re.sub(r'at .+>', '', repr(self))
            new_hash = hashlib.sha256(str_task.encode('utf-8')).hexdigest()[:10]

            for task in self._task_supervisor.scheduled_tasks:
                if new_hash == task.hash:
                    self.cancel()
                    raise DelayedTaskExeption(f'Same task\'s already scheduled!')

            self.hash = new_hash

            return self
        else:
            raise DelayedTaskExeption(f'Coroutine function is expected, but got {type(coro)}!')

    def __lt__(self, other):
        return self.next_run_at < other.next_run_at

    def __repr__(self):
        format_time = lambda t: t.strftime('%Y-%m-%d %H:%M:%S') if t else '[never]'

        timestats = f'last run: {format_time(self.last_run_at)},' \
                    f' next run: {format_time(self.next_run_at)}'

        repeat_stats = f'done: {self.done_times}'
        repeat_stats += f'/{self.repeat}' if self.repeat else ''
        repeat_stats += f', failed: {self.failed_times}'
        repeat_stats += f'/{self.max_failures}' if self.max_failures else ''

        pre = ''
        if self.is_cancelled:
            pre = 'Cancelled - '
        elif self.is_paused:
            pre = 'Paused - '

        return f'    {pre}Task {repeat_stats} {self._get_coro_desc(self._coro)}\n    {timestats}'

    @staticmethod
    def _get_coro_desc(coro):
        if hasattr(coro, '__name__'):
            coro_name = coro.__name__
        else:
            coro_name = repr(coro)

        return coro_name

    @property
    def is_cancelled(self):
        return self._future.cancelled() and not self.is_paused

    @property
    def result(self):
        return self._future.result()

    @property
    def is_done(self):
        return self._future.done()


class MongoConnector(MongoDequeReflection):
    @classmethod
    def create(cls, db_name='tasks_db',
               col_name='async_shed',
               obj_ref=None,
               key='sched_tasks',
               *args, **kwargs):

        self = cls.__new__(cls)
        client = motor.motor_asyncio.AsyncIOMotorClient(*args, **kwargs)
        self.is_connected = False

        self.db = client[db_name]
        self.col = self.db[col_name]

        if obj_ref is None:
            obj_ref = {'array_id': 'tasks_queue'}
        self.obj_ref = obj_ref

        self.key = key
        return self

    async def connect(self, lst=list()):
        await super().create(lst, self, dumps=self._dump_task)
        self.is_connected = True
        return self

    @staticmethod
    def _dump_task(task):
        cached_attrs = ('hash', 'repeat', 'interval', 'is_paused',
                        'next_run_at', 'last_run_at', 'done_times',
                        'failed_times')

        return {attr: task.__dict__[attr] for attr in cached_attrs
                if task.__dict__[attr] is not None}

    async def _sync_prop(self, inst_hash, name, value):
        ref = self.obj_ref.copy()
        ref.update({f'{self.key}': {'$elemMatch': {'hash': inst_hash}}})
        await self.col.update_one(ref, {'$set': {f'{self.key}.$.{name}': value}})

    async def _mongo_remove(self, task):
        task_hash = getattr(task, 'hash', None) or task['hash']
        h = random.getrandbits(32)

        ref = self.obj_ref.copy()
        ref.update({f'{self.key}': {'$elemMatch': {'hash': task_hash}}})
        await self.col.update_one(ref, {'$set': {f'{self.key}.$': h}})

        await self.col.update_one(self.obj_ref, {'$pull': {f'{self.key}': h}})


class AsyncShed:

    loop_resolution = 0.1

    def __init__(self, loop=None, conector=None):
        self.quenue_mutex = asyncio.Lock()
        self.loop = loop or asyncio.get_event_loop()
        self._db_connector = conector

        self._runc = lambda coro: loop.run_until_complete(coro)
        if conector is None:
            self.scheduled_tasks = deque()
        else:
            self.scheduled_tasks = self._runc(conector.connect())

        self.cached_tasks = {}
        self._runc(self._get_cached_tasks())

    async def _get_cached_tasks(self):
        for task in list(self.scheduled_tasks):
            if isinstance(task, dict):
                ct = task.copy()
                self.cached_tasks[ct.pop('hash')] = ct
                self.scheduled_tasks.remove(task)

    def _check_hash(self):
        pass

    async def _new_task(self, **kwargs):
        async with self.quenue_mutex:
            task = DelayedTask(self.loop, db_connector=self._db_connector, supervisor=self, **kwargs)
            self.scheduled_tasks.append(task)
            return task

    def once(self):
        return self._runc(self._new_task())

    def every(self, interval=1, repeat=None, max_failures=None):

        if type(interval) is str:
            interval = self._parse_interval_string(interval)

        elif type(interval) is not int:
            raise ShedulerExeption('Wrong interval string format! ex.1mo1w1d1h1m1s!')

        return self._runc(self._new_task(interval=interval, repeat=repeat, max_failures=max_failures))

    def next_task_scheduled(self):
        if not len(self.scheduled_tasks):
            return None
        try:
            next_task = min(self.scheduled_tasks)
        except TypeError:
            raise ShedulerExeption('Some tasks haven\'t been sheduled yet! Please wait!')
        else:
            return next_task

    async def add_task(self, task):
        if not isinstance(task, DelayedTask):
            raise ShedulerExeption(f'DelayedTask instance is expected, but recieved {type(task)}!')

        if task in self.scheduled_tasks:
            raise ShedulerExeption('Task is already sheduled!')

        if (not task.interval and not task.next_run_at) or not task._coro:
            raise ShedulerExeption(f'{task} is not prepared!'
                                   f' Task should have coro, exec interval or fixed exec time!')

        async with self.quenue_mutex:
            task.done_times = 0
            self.scheduled_tasks.append(task)

        return task

    async def run(self):

        while True:
            await asyncio.sleep(self.loop_resolution)
            async with self.quenue_mutex:

                iter_tasks = list(self.scheduled_tasks)
                for task in iter_tasks:

                    if not task._coro or task.is_paused:
                        continue

                    if self.cached_tasks and task.hash in self.cached_tasks.keys():
                        self._set_task_from_cache(task)

                    if task.is_cancelled:
                        self.scheduled_tasks.remove(task)
                        continue

                    if task._is_idle:
                        log.debug('Scheduling task from main asched loop...')
                        task._schedule()
                        continue

                    if task.is_done:
                        try:
                            result = task.result
                        except Exception as e:
                            log.warning(f'Task failed:\n{task}\n', exc_info=e)
                        else:
                            log.info(f'Task finished with result: {result}\n{task} ')

                        if task._should_be_reshedulled:
                            log.debug('Rescheduling task from main asched loop...')
                            task._reshedule()
                        else:
                            task._set_default_stats()
                            self.scheduled_tasks.remove(task)

    def _set_task_from_cache(self, task):
        ct = self.cached_tasks.pop(task.hash)
        if ct['done_times'] == task.repeat or ct['failed_times'] == task.max_failures:
            return

        task.is_paused = ct['is_paused']
        task.done_times = ct['done_times']
        task.failed_times = ct['failed_times']
        task.last_run = ct.get('last_run', None)
        if not task.interval and task.next_run < ct['next_run']:
            task.next_run = ct['next_run']

    @staticmethod
    def _parse_interval_string(interval):

        interval_re = re.compile(r'^(?=(?:\d+(?:[wdhms]|mo)?)+$)'
                                 r'(?:(?P<months>\d+)mo)?(?:(?P<weeks>\d+)w)?'
                                 r'(?:(?P<days>\d+)d)?(?:(?P<hours>\d+)h)?'
                                 r'(?:(?P<minutes>\d+)m)?(?:(?P<seconds>\d+)s)?$')

        match = re.match(interval_re, interval)

        if not match:
            raise ShedulerExeption('Wrong interval string format! ex.1mo1w1d1h1m1s!')

        def get_match(gname):
            val = match.group(gname)
            return int(val) if val else 0

        interval = 2592000 * get_match('months') + 604800 * get_match('weeks') \
                   + 86400 * get_match('days') + 3600 * get_match('hours') \
                   + 60 * get_match('minutes') + get_match('seconds')

        if interval == 0:
            raise ShedulerExeption('Can\'t set zero interval!')

        return interval
