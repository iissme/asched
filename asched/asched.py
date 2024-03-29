import asyncio
import logging
import random
import weakref
import re
from collections import deque
from datetime import datetime, timedelta, time
from hashlib import sha256
from functools import partial

import motor.motor_asyncio

from asyncio_mongo_reflection import MongoDequeReflection

log = logging.getLogger(__name__)


class DelayedTaskExeption(Exception):
    pass


class WrongTimeDataException(Exception):
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
                if instance._db_connector is not None:
                    if self.name == '_coro':
                        sync_name = 'runs'
                        sync_value = instance._get_coro_desc(value)

                    instance.__dict__['_sync_prop_task'] = instance._loop.create_task(
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
                 db_connector=None,
                 coro=None,
                 interval=None,
                 repeat=None,
                 max_failures=None,
                 exc_type=None,
                 supervisor=None):

        self._loop = loop
        self._db_connector = db_connector
        self._task_supervisor = supervisor
        self._sync_prop_task = None
        self._coro = coro
        self._future = asyncio.Future(loop=loop)

        self.repeat = repeat
        self.interval = interval
        self.max_failures = max_failures
        self.exc_type = exc_type if exc_type else Exception
        self._set_default_stats()

        self.hash = sha256(str(hash(self)).encode('utf-8')).hexdigest()[:10]

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
                    if isinstance(e, self.exc_type):
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
            self._future = asyncio.Future(loop=self._loop)

        now = datetime.now()
        if self.interval:
            delta = timedelta(seconds=self.interval)
            if self.next_run_at and (not self.last_run_at
                                     or (not self.done_times and not self.failed_times)):
                pass  # from cache
            elif self.last_run_at and (self.last_run_at + delta) > now:
                self.next_run_at = self.last_run_at + delta
            else:
                self.next_run_at = now + delta

        if self.next_run_at < now:
            self.cancel()
            raise WrongTimeDataException('Passed date should be later then \'now\'!')

        delta = self.next_run_at - now
        loop_correction = getattr(self._task_supervisor, 'loop_resolution', 0)
        run_at = self._loop.time() + delta.total_seconds() - loop_correction

        self.timer = self._loop.call_at(run_at, self._callback)
        log.info(f'Task is sheduled!\n{self}')

    def _reshedule(self):
        self.last_run_at = self.next_run_at
        self._schedule()

    async def cancel(self):
        async with self._task_supervisor._iteration_mutex:
            self._future.cancel()
            self._should_be_reshedulled = False  # in case task is done before cancel

    async def pause(self):
        async with self._task_supervisor._iteration_mutex:
            if self.is_paused or self.is_cancelled:
                raise DelayedTaskExeption('Only running tasks can be paused!')

            self.is_paused = True
            self._future.cancel()
            log.info(f'Task({self.hash}) is paused!')

    async def resume(self):
        async with self._task_supervisor._iteration_mutex:
            if not self.is_paused:
                raise DelayedTaskExeption('You can\'t resume task that is not paused!')

            log.info(f'Task({self.hash}) resumed!')
            self.is_paused = False
            self._schedule()

    def at(self, sched_date):

        if type(sched_date) is str:
            sched_date = self._parse_datestr(sched_date)
        elif not isinstance(sched_date, datetime):
            raise WrongTimeDataException('Datetime or str instance is expected!')

        self.next_run_at = sched_date

        # allow task self invoke
        if self._coro:
            self._schedule()

        return self

    async def run(self, coro, *args, **kwargs):
        if asyncio.iscoroutinefunction(coro):
            async with self._task_supervisor._iteration_mutex:
                self._coro = partial(coro, *args, **kwargs)
                # make sure we've set coro desc in db before new hash
                await asyncio.wait_for(self._sync_prop_task, timeout=None)

                new_hash = sha256(self._hash_str.encode('utf-8')).hexdigest()[:10]

                for task in self._task_supervisor.scheduled_tasks:
                    if new_hash == task.hash:
                        self.cancel()
                        raise DelayedTaskExeption(f'Same task\'s already scheduled!')

                self.hash = new_hash

            return self
        else:
            raise DelayedTaskExeption(f'Coroutine function is expected, but got {type(coro)}!')

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

        return f'    {pre}Task({self.hash}) {repeat_stats}' \
               f' {self._get_coro_desc(self._coro)}\n    {timestats}'

    @staticmethod
    def _parse_datestr(dstr):
        hh_mm_re = re.compile(r'^(?P<hours>\d{,2}):(?P<minutes>\d{,2})(:(?P<seconds>\d{,2}))?$')
        match = re.match(hh_mm_re, dstr)
        if match:
            get_int = lambda tstr: int(tstr[1:] if len(tstr) == 2 and tstr.startswith('0') else tstr)
            hour = get_int(match.group('hours'))
            minute = get_int(match.group('minutes'))
            second = get_int(match.group('seconds') or '0')

            if not (0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59):
                raise WrongTimeDataException('Wrong time string value! Supposed is hh(0-23):mm(0-59):ss(0-59)')

            set_time = datetime.combine(datetime.now(), time(hour, minute, second))
            if set_time < datetime.now():
                set_time += timedelta(days=1)
            return set_time
        else:
            raise WrongTimeDataException('hh:mm:ss formated string is expected!')

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

    @property
    def _hash_str(self):
        task_info = f'{self.interval}{self.repeat}{self.max_failures}'
        if self._coro:
            task_info += re.sub(r'at .+>', '', self._get_coro_desc(self._coro))
        return task_info


class MongoConnector(MongoDequeReflection):

    async def __ainit__(self, db_name='tasks_db', col_name='async_sched',
                        obj_ref=None, key='sched_tasks', *args, **kwargs):

        client = motor.motor_asyncio.AsyncIOMotorClient(*args, **kwargs)
        self.db = client[db_name]
        self.col = self.db[col_name]

        if obj_ref is None:
            obj_ref = {'array_id': 'tasks_queue'}
        self.obj_ref = obj_ref
        self.key = key

        await super().__ainit__(dumps=self._dump_task)

    @staticmethod
    def _dump_task(task):
        if not isinstance(task, DelayedTask):
            return task

        cached_attrs = ('hash', 'repeat', 'interval', 'is_paused',
                        'next_run_at', 'last_run_at', 'done_times',
                        'failed_times')

        return {attr: task.__dict__[attr] for attr in cached_attrs
                if task.__dict__[attr] is not None}

    async def _sync_prop(self, inst_hash, name, value):
        ref = self.obj_ref.copy()
        ref.update({f'{self.key}': {'$elemMatch': {'hash': inst_hash}}})
        await self.mongo_pending.join()
        await self.col.update_one(ref, {'$set': {f'{self.key}.$.{name}': value}})


def asyncinit(cls):
    __cnew__ = cls.__new__

    async def init(obj, *args, **kwargs):
        await obj.__ainit__(*args, **kwargs)
        return obj

    def new(cls, *args, **kwargs):
        obj = __cnew__(cls)
        coro = init(obj, *args, **kwargs)
        return coro

    cls.__new__ = new
    return cls


@asyncinit
class AsyncSched:

    async def __ainit__(self, loop=None, conector=None, loop_resolution=0.1):
        self.loop_resolution = loop_resolution
        self._iteration_mutex = asyncio.Lock()
        self.loop = loop or asyncio.get_event_loop()
        self._db_connector = await conector if conector else None
        self.scheduled_tasks = self._db_connector if conector else deque()
        self.cached_tasks = {}

        await self._get_cached_tasks()
        self._loop_task = self.loop.create_task(self._scheduler_loop())
        self._loop_task.add_done_callback(self._handle_loop_stop)
        self._finalizer = weakref.finalize(self, self._cancel_loop_task)

    async def _get_cached_tasks(self):
        for task in list(self.scheduled_tasks):
            if isinstance(task, dict):
                ct = task.copy()
                self.cached_tasks[ct.pop('hash')] = ct
                self.scheduled_tasks.remove(task)

        await self.scheduled_tasks.mongo_pending.join()

    def _new_task(self, **kwargs):
        task = DelayedTask(self.loop, db_connector=self._db_connector, supervisor=self, **kwargs)
        self.scheduled_tasks.append(task)
        return task

    def once(self, at=None):
        task = self._new_task()
        return task if not at else task.at(at)

    def every(self,
              interval,
              repeat=None,
              max_failures=None,
              exc_type=None,
              start_at=None):

        if type(interval) is str:
            interval = self._parse_interval_string(interval)
        elif type(interval) is not int:
            raise ShedulerExeption('Wrong interval string format! ex.1mo1w1d1h1m1s!')

        if not max_failures and exc_type:
            raise ShedulerExeption('max_failures argument is required with exc_type!')

        task = self._new_task(interval=interval, repeat=repeat,
                              max_failures=max_failures, exc_type=exc_type)

        return task if not start_at else task.at(start_at)

    def next_task_scheduled(self):
        if not len(self.scheduled_tasks):
            return None

        def get_key(o):
            key = getattr(o, 'next_run_at', None)
            if not key:
                key = datetime(year=9999, month=1, day=1)  # task not scheduled or not active
            return key

        next_task = min(self.scheduled_tasks, key=get_key)
        return next_task

    async def add_task(self, task):
        if not isinstance(task, DelayedTask):
            raise ShedulerExeption(f'DelayedTask instance is expected, but recieved {type(task)}!')

        if task in self.scheduled_tasks:
            raise ShedulerExeption('Task is already sheduled!')

        if (not task.interval and not task.next_run_at) or not task._coro:
            raise ShedulerExeption(f'{task} is not prepared!'
                                   f' Task should have coro, exec interval or fixed exec time!')

        task.done_times = 0
        self.scheduled_tasks.append(task)

        return task

    @staticmethod
    def _handle_loop_stop(future):
        try:
            future.result()
        except Exception as e:
            trace = getattr(future, '_source_traceback', None)
            full_trace = ''.join(trace.format()) if trace else 'Not available.'
            log.error(f'Asched loop error!\nFull traceback:\n{full_trace}\n'
                      f'Exc info:\n', exc_info=e)
            raise

    def _cancel_loop_task(self):
        if not self.loop.is_closed():
            self._loop_task.cancel()

    async def _scheduler_loop(self):

        while True:
            await asyncio.sleep(self.loop_resolution)
            if not len(self.scheduled_tasks):
                continue

            await self._iteration_mutex.acquire()

            for task in self.scheduled_tasks:

                if not task._coro:
                    continue

                if self.cached_tasks and task.hash in self.cached_tasks.keys():
                    self._set_task_from_cache(task)

                if task.is_paused:
                    continue

                if task.is_cancelled:
                    self.loop.call_soon(partial(self.scheduled_tasks.remove, task))
                    task._set_default_stats()
                    continue

                if task._is_idle:
                    log.debug(f'Scheduling task({task.hash}) from main asched loop...')
                    self.loop.call_soon(task._schedule)
                    continue

                if task.is_done:
                    try:
                        result = task.result
                    except Exception as e:
                        log.warning(f'Task failed:\n{task}\n', exc_info=e)
                    else:
                        log.info(f'Task finished with result: {result}\n{task} ')

                    if task._should_be_reshedulled:
                        log.debug(f'Rescheduling task({task.hash}) from main asched loop...')
                        self.loop.call_soon(task._reshedule)
                    else:
                        task._set_default_stats()
                        self.loop.call_soon(partial(self.scheduled_tasks.remove, task))

            self._iteration_mutex.release()

    def _set_task_from_cache(self, task):

        log.info(f'Setting task({task.hash}) from cache...')
        ct = self.cached_tasks.pop(task.hash)
        if ct['done_times'] == task.repeat or ct['failed_times'] == task.max_failures:
            return

        task.is_paused = ct['is_paused']
        task.done_times = ct['done_times']
        task.failed_times = ct['failed_times']
        task.last_run_at = ct.get('last_run_at', None)

        ct_next_run = ct.get('next_run_at', None)
        if ct_next_run and ct_next_run > datetime.now():
            if not task.next_run_at or task.next_run_at > ct_next_run:
                task.next_run_at = ct_next_run

        log.info(f'Task is set from cache:\n{task} ')

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
