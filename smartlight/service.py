"""Actor boundary: HTTP futures + MQTT callback events, one SQLite owner."""

import asyncio
import logging
import queue
import sqlite3
import threading
from concurrent.futures import Future

from .engine import Clock, Engine
from .errors import DomainError

LOG = logging.getLogger(__name__)


class Service:
    def __init__(self, settings, transport, clock=None):
        self.settings, self.transport = settings, transport
        self.clock = clock or Clock()
        self.jobs = queue.Queue(maxsize=settings.queue_size)
        self.stop_event = threading.Event()
        self.started = Future()
        self.dropped = 0
        self.generation = 0
        self._counter_lock = threading.Lock()
        self.thread = None
        self.engine = None

    def start(self):
        self.thread = threading.Thread(target=self._work, name="sqlite-domain-worker", daemon=True)
        self.thread.start()
        self.started.result(timeout=5)
        self.transport.start(self.ingest)

    def stop(self):
        self.transport.stop()
        self.stop_event.set()
        if self.thread:
            self.thread.join(5)
            if self.thread.is_alive():
                raise RuntimeError("Database worker did not stop within 5s")

    def ingest(self, topic, payload, session):
        with self._counter_lock:
            generation = self.generation
        args = (topic, payload, self.clock.utc(), self.clock.mono(), session, generation)
        try:
            self.jobs.put_nowait(("ingest", args, {}, None))
            return True
        except queue.Full:
            with self._counter_lock:
                self.dropped += 1
                self.generation += 1
            return False

    async def call(self, operation, *args, **kwargs):
        future = Future()
        try:
            self.jobs.put_nowait((operation, args, kwargs, future))
        except queue.Full:
            raise DomainError(503, "Processing queue full") from None
        try:
            return await asyncio.wait_for(asyncio.wrap_future(future), timeout=3)
        except TimeoutError:
            future.cancel()
            raise DomainError(
                503, "Database request deadline exceeded; check node pending_command_id before retrying"
            ) from None

    def _database_error(self):
        self.engine.db_ok = False
        try:
            self.engine.db.rollback()
        except sqlite3.Error:
            pass

    def _work(self):
        observed_drops = 0
        try:
            self.engine = Engine(self.settings.database, self.transport, self.clock)
            self.started.set_result(True)
        except Exception as error:
            self.started.set_exception(error)
            return
        try:
            while not self.stop_event.is_set():
                try:
                    if self.dropped != observed_drops:
                        self.engine.pipeline_gap()
                        observed_drops = self.dropped
                    if self.engine.broker != self.transport.connected or self.engine.session != self.transport.session:
                        self.engine.connection(self.transport.connected, self.transport.session)
                    if self.jobs.qsize() < max(1, self.settings.queue_size // 2):
                        self.engine.queue_ok = True
                    try:
                        operation, args, kwargs, future = self.jobs.get(timeout=0.05)
                    except queue.Empty:
                        self.engine.tick()
                        continue
                    if future is not None and not future.set_running_or_notify_cancel():
                        continue
                    try:
                        target = self.engine
                        for part in operation.split("."):
                            target = getattr(target, part)
                        result = target(*args, **kwargs)
                        if future is not None:
                            future.set_result(result)
                    except Exception as error:
                        if isinstance(error, sqlite3.Error):
                            self._database_error()
                        if future is not None:
                            future.set_exception(
                                DomainError(503, "Database unavailable") if isinstance(error, sqlite3.Error) else error
                            )
                        else:
                            LOG.exception("MQTT event processing failed")
                    self.engine.tick()
                except sqlite3.Error:
                    self._database_error()
                    LOG.exception("Database worker tick failed")
        finally:
            self.engine.close()
            while not self.jobs.empty():
                _op, _args, _kwargs, future = self.jobs.get_nowait()
                if future is not None and not future.done():
                    future.set_exception(DomainError(503, "Backend stopping"))
