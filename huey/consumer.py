import datetime
import logging
import os
import signal
import socket
import sys
import threading
import time

from multiprocessing import Event as ProcessEvent
from multiprocessing import Process

try:
    import gevent
    from gevent import Greenlet
    from gevent import monkey
    from gevent.event import Event as GreenEvent
except ImportError:
    Greenlet = GreenEvent = None

from huey.constants import WORKER_GREENLET
from huey.constants import WORKER_PROCESS
from huey.constants import WORKER_THREAD
from huey.constants import WORKER_TYPES
from huey.exceptions import ConfigurationError
from huey.utils import time_clock


class ConsumerStopped(Exception): pass


class BaseProcess(object):
    process_name = 'BaseProcess'

    def __init__(self, huey):
        self.huey = huey
        self._logger = self.create_logger()

    def create_logger(self):
        return logging.getLogger('huey.consumer.%s' % self.process_name)

    def initialize(self):
        pass

    def sleep_for_interval(self, start_ts, nseconds):
        """
        Sleep for a given interval with respect to the start timestamp.

        So, if the start timestamp is 1337 and nseconds is 10, the method will
        actually sleep for nseconds - (current_timestamp - start_timestamp). So
        if the current timestamp is 1340, we'll only sleep for 7 seconds (the
        goal being to sleep until 1347, or 1337 + 10).
        """
        sleep_time = nseconds - (time_clock() - start_ts)
        if sleep_time <= 0:
            return
        self._logger.debug('Sleeping for %s', sleep_time)
        # Recompute time to sleep to improve accuracy in case the process was
        # pre-empted by the kernel while logging.
        sleep_time = nseconds - (time_clock() - start_ts)
        if sleep_time > 0:
            time.sleep(sleep_time)

    def loop(self, now=None):
        """
        Process-specific implementation. Called repeatedly for as long as the
        consumer is running. The `now` parameter is currently only used in the
        unit-tests (to avoid monkey-patching datetime / time). Return value is
        ignored, but an unhandled exception will lead to the process exiting.
        """
        raise NotImplementedError

    def shutdown(self):
        """
        Called when process has finished running (e.g., when shutting down).
        """
        pass


class Worker(BaseProcess):
    """
    Worker implementation.

    Will pull tasks from the queue, executing them or adding them to the
    schedule if they are set to run in the future.
    """
    process_name = 'Worker'

    def __init__(self, huey, default_delay, max_delay, backoff):
        self.delay = self.default_delay = default_delay
        self.max_delay = max_delay
        self.backoff = backoff
        super(Worker, self).__init__(huey)

    def initialize(self):
        for name, startup_hook in self.huey._startup.items():
            self._logger.debug('calling startup hook "%s"', name)
            try:
                startup_hook()
            except Exception as exc:
                self._logger.exception('startup hook "%s" failed', name)

    def shutdown(self):
        for name, shutdown_hook in self.huey._shutdown.items():
            self._logger.debug('calling shutdown hook "%s"', name)
            try:
                shutdown_hook()
            except Exception as exc:
                self._logger.exception('shutdown hook "%s" failed', name)

    def loop(self, now=None):
        task = None
        try:
            task = self.huey.dequeue()
        except Exception:
            self._logger.exception('Error reading from queue')
            self.sleep()
        else:
            if task is not None:
                self.delay = self.default_delay
                try:
                    self.huey.execute(task, now)
                except Exception as exc:
                    self._logger.exception('Unhandled error during execution '
                                           'of task %s.', task.id)
            elif not self.huey.storage.blocking:
                self.sleep()

    def sleep(self):
        if self.delay > self.max_delay:
            self.delay = self.max_delay

        time.sleep(self.delay)
        self.delay *= self.backoff


class Scheduler(BaseProcess):
    """
    Scheduler handles enqueueing tasks when they are scheduled to execute. Note
    that the scheduler does not actually execute any tasks, but simply enqueues
    them so that they can be picked up by the worker processes.

    If periodic tasks are enabled, the scheduler will wake up every 60 seconds
    to enqueue any periodic tasks that should be run.
    """
    periodic_task_seconds = 60
    process_name = 'Scheduler'

    def __init__(self, huey, interval, periodic):
        super(Scheduler, self).__init__(huey)
        self.interval = max(min(interval, 60), 1)

        self.periodic = periodic
        self._next_loop = time_clock()
        self._next_periodic = time_clock()

    def loop(self, now=None):
        current = self._next_loop
        self._next_loop += self.interval
        if self._next_loop < time_clock():
            self._logger.debug('scheduler skipping iteration to avoid race.')
            return

        try:
            task_list = self.huey.read_schedule(now)
        except Exception:
            self._logger.exception('Error reading schedule.')
        else:
            for task in task_list:
                self._logger.debug('Enqueueing %s', task)
                self.huey.enqueue(task)

        if self.periodic and self._next_periodic <= time_clock():
            self._next_periodic += self.periodic_task_seconds
            self.enqueue_periodic_tasks(now)

        self.sleep_for_interval(current, self.interval)

    def enqueue_periodic_tasks(self, now):
        self._logger.debug('Checking periodic tasks')
        for task in self.huey.read_periodic(now):
            self._logger.info('Enqueueing periodic task %s.', task)
            self.huey.enqueue(task)


class Environment(object):
    """
    Provide a common interface to the supported concurrent environments.
    """
    def get_stop_flag(self):
        raise NotImplementedError

    def create_process(self, runnable, name):
        raise NotImplementedError

    def is_alive(self, proc):
        raise NotImplementedError


class ThreadEnvironment(Environment):
    def get_stop_flag(self):
        return threading.Event()

    def create_process(self, runnable, name):
        t = threading.Thread(target=runnable, name=name)
        t.daemon = True
        return t

    def is_alive(self, proc):
        return proc.is_alive()


class GreenletEnvironment(Environment):
    def get_stop_flag(self):
        return GreenEvent()

    def create_process(self, runnable, name):
        def run_wrapper():
            gevent.sleep()
            runnable()
            gevent.sleep()
        return Greenlet(run=run_wrapper)

    def is_alive(self, proc):
        return not proc.dead


class ProcessEnvironment(Environment):
    def get_stop_flag(self):
        return ProcessEvent()

    def create_process(self, runnable, name):
        p = Process(target=runnable, name=name)
        p.daemon = True
        return p

    def is_alive(self, proc):
        return proc.is_alive()


WORKER_TO_ENVIRONMENT = {
    WORKER_THREAD: ThreadEnvironment,
    WORKER_GREENLET: GreenletEnvironment,
    WORKER_PROCESS: ProcessEnvironment,
}


class Consumer(object):
    """
    Consumer sets up and coordinates the execution of the workers and scheduler
    and registers signal handlers.
    """
    # Simplify providing custom implementations. See _create_worker and
    # _create_scheduler if you need more sophisticated overrides.
    worker_class = Worker
    scheduler_class = Scheduler

    def __init__(self, huey, workers=1, periodic=True, initial_delay=0.1,
                 backoff=1.15, max_delay=10.0, scheduler_interval=1,
                 worker_type=WORKER_THREAD, check_worker_health=True,
                 health_check_interval=10, flush_locks=False,
                 extra_locks=None, drain_timeout=None):

        self._logger = logging.getLogger('huey.consumer')
        if huey.immediate:
            self._logger.warning('Consumer initialized with Huey instance '
                                 'that has "immediate" mode enabled. This '
                                 'must be disabled before the consumer can '
                                 'be run.')
        if drain_timeout is not None and drain_timeout < 0:
            raise ConfigurationError('drain_timeout must be >= 0')
        self.huey = huey
        self.workers = workers  # Number of workers.
        self.periodic = periodic  # Enable periodic task scheduler?
        self.default_delay = initial_delay  # Default queue polling interval.
        self.backoff = backoff  # Exponential backoff factor when queue empty.
        self.max_delay = max_delay  # Maximum interval between polling events.

        # When a drain timeout is specified, SIGTERM (or a graceful shutdown)
        # will stop the workers from dequeueing new tasks and wait up to this
        # many seconds for in-flight tasks to finish. Any tasks that are
        # still unacknowledged afterwards are released back to the queue
        # (subject to the capabilities of the storage backend).
        self.drain_timeout = drain_timeout

        # Ensure that the scheduler runs at an interval between 1 and 60s.
        self.scheduler_interval = max(min(scheduler_interval, 60), 1)
        if 60 % self.scheduler_interval != 0:
            raise ConfigurationError('Scheduler interval must be a factor '
                                     'of 60, e.g. 1, 2, 3, 4, 5, 6, 10, 12...')

        if worker_type == 'gevent': worker_type = WORKER_GREENLET
        if worker_type == WORKER_GREENLET and Greenlet is None:
            raise ImportError('Could not import gevent - is it installed?')
        self.worker_type = worker_type  # What process model are we using?

        # Configure health-check and consumer main-loop attributes.
        self._stop_flag_timeout = 0.1
        self._health_check = check_worker_health
        self._health_check_interval = float(health_check_interval)

        # Create the execution environment helper.
        self.environment = self.get_environment(self.worker_type)

        # Create the event used to signal the process should terminate. We'll
        # also store a boolean flag to indicate whether we should restart after
        # the processes are cleaned up.
        self._received_signal = False
        self._restart = False
        self._graceful = True
        self._force_shutdown = False
        self.stop_flag = self.environment.get_stop_flag()

        # In the event the consumer was killed while running a task that held
        # a lock, this ensures that all locks are flushed before starting.
        if flush_locks or extra_locks:
            lock_names = extra_locks.split(',') if extra_locks else ()
            self.flush_locks(*lock_names)

        # Create the scheduler process (but don't start it yet).
        scheduler = self._create_scheduler()
        self.scheduler = self._create_process(scheduler, 'Scheduler')

        # Create the worker process(es) (also not started yet).
        self.worker_threads = []
        for i in range(workers):
            worker = self._create_worker()
            process = self._create_process(worker, 'Worker-%d' % (i + 1))

            # The worker threads are stored as [(worker impl, worker_t), ...].
            # The worker impl is not currently referenced in any consumer code,
            # but it is referenced in the test-suite.
            self.worker_threads.append((worker, process))

    def flush_locks(self, *names):
        self._logger.debug('Flushing locks before starting up.')
        flushed = self.huey.flush_locks(*names)
        if flushed:
            self._logger.warning('Found stale locks: %s' % (
                ', '.join(key for key in flushed)))

    def get_environment(self, worker_type):
        if worker_type not in WORKER_TO_ENVIRONMENT:
            raise ValueError('worker_type must be one of %s.' %
                             ', '.join(WORKER_TYPES))
        return WORKER_TO_ENVIRONMENT[worker_type]()

    def _create_worker(self):
        return self.worker_class(
            huey=self.huey,
            default_delay=self.default_delay,
            max_delay=self.max_delay,
            backoff=self.backoff)

    def _create_scheduler(self):
        return self.scheduler_class(
            huey=self.huey,
            interval=self.scheduler_interval,
            periodic=self.periodic)

    def _create_process(self, process, name):
        """
        Repeatedly call the `loop()` method of the given process. Unhandled
        exceptions in the `loop()` method will cause the process to terminate.
        """
        def _run():
            if self.worker_type == WORKER_PROCESS:
                self._set_child_signal_handlers()

            process.initialize()
            try:
                while not self.stop_flag.is_set():
                    process.loop()
            except KeyboardInterrupt:
                pass
            except:
                self._logger.exception('Process %s died!', name)
            finally:
                process.shutdown()
                if (self.worker_type == WORKER_PROCESS and
                        self.drain_timeout is not None):
                    # This code runs in the child process, so any tasks it
                    # dequeued but did not acknowledge are only visible here.
                    # Release them so they can be picked up by another
                    # consumer instead of being lost.
                    released = self.huey.release_unacked_tasks()
                    if released:
                        self._logger.info(
                            '%s released %d unacknowledged task(s) back to '
                            'the queue.', name, len(released))
        return self.environment.create_process(_run, name)

    def start(self):
        """
        Start all consumer processes and register signal handlers.
        """
        if self.huey.immediate:
            raise ConfigurationError(
                'Consumer cannot be run with Huey instances where immediate '
                'is enabled. Please check your configuration and ensure that '
                '"huey.immediate = False".')

        # Check if gevent is used, and if monkey-patch applied properly.
        if self.worker_type == WORKER_GREENLET:
            if not monkey.is_module_patched('socket'):
                self._logger.warning('Gevent monkey-patch has not been applied'
                                     ', this may result in incorrect or '
                                     'unpredictable behavior.')

        # Log startup message.
        self._logger.info('Huey consumer started with %s %s, PID %s at %s',
                          self.workers, self.worker_type, os.getpid(),
                          self.huey._get_timestamp())
        self._logger.info('Scheduler runs every %s second(s).',
                          self.scheduler_interval)
        self._logger.info('Periodic tasks are %s.',
                          'enabled' if self.periodic else 'disabled')

        msg = ['The following commands are available:']
        for command in self.huey._registry._registry:
            msg.append('+ %s' % command)

        self._logger.info('\n'.join(msg))

        # Take over any tasks that were dequeued by a previous consumer that
        # is no longer running (if the storage backend supports it).
        self._reclaim_unacked_tasks()

        # Start the scheduler and workers.
        self.scheduler.start()
        for _, worker_process in self.worker_threads:
            worker_process.start()

        # Finally set the signal handlers for main process.
        self._set_signal_handlers()

    def _reclaim_unacked_tasks(self):
        storage = self.huey.storage
        try:
            owners = storage.unacked_owners()
        except Exception:
            self._logger.exception('Error checking for unacknowledged tasks.')
            return

        # Only reclaim tasks whose owner is no longer running. Claims held by
        # live consumers (e.g. another consumer sharing the same queue) are
        # left alone.
        dead_owners = [owner for owner in owners
                       if not self._owner_is_alive(owner)]
        if not dead_owners:
            return

        try:
            reclaimed = storage.reclaim_unacked(dead_owners)
        except Exception:
            self._logger.exception('Error reclaiming unacknowledged tasks.')
        else:
            if reclaimed:
                self._logger.warning(
                    'Reclaimed %d unacknowledged task(s) from %d dead '
                    'consumer(s): %s', reclaimed, len(dead_owners),
                    ', '.join(dead_owners))

    def _owner_is_alive(self, owner):
        # Owner identifiers are of the form "hostname:pid:token". If the
        # format is unrecognized, or the owner resides on another host, we
        # conservatively assume it is alive and leave its claims alone.
        try:
            host, pid, _ = owner.rsplit(':', 2)
            pid = int(pid)
        except (AttributeError, ValueError):
            return True
        if host != socket.gethostname() or pid == os.getpid():
            return True
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except OSError:
            # Insufficient permissions or unsupported platform: assume alive.
            return True
        return True

    def stop(self, graceful=False):
        """
        Set the stop-flag.

        If `graceful=True`, this method blocks until the workers to finish
        executing any tasks they might be currently working on.
        """
        self.stop_flag.set()
        if graceful:
            if self.drain_timeout is not None:
                self._logger.info(
                    'Draining: no new tasks will be dequeued, waiting up to '
                    '%0.1fs for in-flight tasks to finish...',
                    self.drain_timeout)
            else:
                self._logger.info('Shutting down gracefully...')
            try:
                stopped = self._join_processes(self.drain_timeout)
            except KeyboardInterrupt:
                self._logger.info('Received request to shut down now.')
                self._restart = False
            else:
                if stopped:
                    self._logger.info('All workers have stopped.')
                else:
                    self._logger.warning(
                        'Timed out or interrupted while waiting for workers '
                        'to finish.')
        else:
            self._logger.info('Shutting down')

        if self.drain_timeout is not None:
            # Return any dequeued-but-unacknowledged tasks to the queue, so
            # they can be picked up by another consumer. Tasks that finished
            # executing and are being finalized (result store, retry,
            # callbacks) are excluded, so a task is never both acknowledged
            # and re-queued.
            released = self.huey.release_unacked_tasks()
            if released:
                self._logger.info('Released %d unacknowledged task(s) back '
                                  'to the queue.', len(released))

    def _join_processes(self, timeout=None):
        """
        Wait for the worker and scheduler processes to exit, returning True
        if they all stopped. If a timeout is specified, give up once the
        timeout has expired. A forced shutdown (e.g. a second SIGTERM) also
        causes an early return.
        """
        deadline = None if timeout is None else time_clock() + timeout
        processes = [process for _, process in self.worker_threads]
        processes.append(self.scheduler)
        for process in processes:
            while self.environment.is_alive(process):
                if self._force_shutdown:
                    return False
                wait = 0.1
                if deadline is not None:
                    remaining = deadline - time_clock()
                    if remaining <= 0:
                        return False
                    wait = min(wait, remaining)
                process.join(wait)
        return True

    def run(self):
        """
        Run the consumer.
        """
        self.start()
        health_check_ts = time_clock()

        while True:
            try:
                health_check_ts = self.loop(health_check_ts)
            except ConsumerStopped:
                break

        self.huey.notify_interrupted_tasks()

        if self._restart:
            self._logger.info('Consumer will restart.')
            python = sys.executable
            os.execl(python, python, *sys.argv)
        else:
            self._logger.info('Consumer exiting.')

    def loop(self, health_check_ts=None):
        try:
            self.stop_flag.wait(timeout=self._stop_flag_timeout)
        except KeyboardInterrupt:
            self._logger.info('Received SIGINT')
            self.stop(graceful=True)
        except:
            self._logger.exception('Error in consumer.')
            self.stop()
        else:
            if self._received_signal:
                self.stop(graceful=self._graceful)

        if self.stop_flag.is_set():
            # Flag to caller that the main consumer loop should shut down.
            raise ConsumerStopped

        if self._health_check and health_check_ts:
            now = time_clock()
            if now >= health_check_ts + self._health_check_interval:
                health_check_ts = now
                self.check_worker_health()

        return health_check_ts

    def check_worker_health(self):
        """
        Check the health of the worker processes. Workers that have died will
        be replaced with new workers.
        """
        self._logger.debug('Checking worker health.')
        workers = []
        restart_occurred = False
        for i, (worker, worker_t) in enumerate(self.worker_threads):
            if not self.environment.is_alive(worker_t):
                self._logger.warning('Worker %d died, restarting.', i + 1)
                worker = self._create_worker()
                worker_t = self._create_process(worker, 'Worker-%d' % (i + 1))
                worker_t.start()
                restart_occurred = True
            workers.append((worker, worker_t))

        if restart_occurred:
            self.worker_threads = workers
        else:
            self._logger.debug('Workers are up and running.')

        if not self.environment.is_alive(self.scheduler):
            self._logger.warning('Scheduler died, restarting.')
            scheduler = self._create_scheduler()
            self.scheduler = self._create_process(scheduler, 'Scheduler')
            self.scheduler.start()
        else:
            self._logger.debug('Scheduler is up and running.')

        return not restart_occurred

    def _set_signal_handlers(self):
        signal.signal(signal.SIGTERM, self._handle_stop_signal)
        if self.worker_type in (WORKER_GREENLET, WORKER_THREAD):
            # Add a special INT handler when using gevent. If the running
            # greenlet is not the main hub, then Gevent will raise a
            # KeyboardInterrupt in the running greenlet by default. This
            # ensures that when INT is received we properly flag the main loop
            # for graceful shutdown and do NOT propagate the exception.
            # This is also added for threads to ensure that, in the event of a
            # SIGHUP followed by a SIGINT, we respect the SIGINT.
            signal.signal(signal.SIGINT, self._handle_interrupt_signal_gevent)
        else:
            signal.signal(signal.SIGINT, signal.default_int_handler)
        if hasattr(signal, 'SIGHUP'):
            signal.signal(signal.SIGHUP, self._handle_restart_signal)

    def _handle_interrupt_signal_gevent(self, sig_num, frame):
        self._logger.info('Received SIGINT')
        self._received_signal = True
        self._restart = False
        self._graceful = True
        signal.signal(signal.SIGINT, signal.default_int_handler)

    def _handle_stop_signal(self, sig_num, frame):
        if self._received_signal and not self._restart:
            # A second SIGTERM indicates we should stop as quickly as
            # possible, rather than waiting for in-flight tasks to drain.
            self._logger.info('Received SIGTERM again, forcing shutdown')
            self._graceful = False
            self._force_shutdown = True
            if self.worker_type == WORKER_GREENLET:
                self._kill_greenlets()
            return
        self._logger.info('Received SIGTERM')
        self._received_signal = True
        self._restart = False
        # When a drain timeout is configured, SIGTERM triggers a graceful
        # drain: workers stop dequeueing new tasks and in-flight tasks are
        # given up to "drain_timeout" seconds to finish. Otherwise the
        # consumer shuts down immediately.
        self._graceful = self.drain_timeout is not None
        if self.worker_type == WORKER_GREENLET and not self._graceful:
            self._kill_greenlets()

    def _kill_greenlets(self):
        def kill_workers():
            gevent.killall([t for _, t in self.worker_threads],
                           KeyboardInterrupt)
        gevent.spawn(kill_workers)

    def _handle_restart_signal(self, sig_num, frame):
        self._logger.info('Received SIGHUP, will restart')
        self._received_signal = True
        self._restart = True

    def _set_child_signal_handlers(self):
        # Install signal handlers in child process. We ignore SIGHUP (restart)
        # and SIGINT (graceful shutdown), as these are handled by the main
        # process. Upon a TERM signal, we raise a KeyboardInterrupt, which is
        # caught below (and in the worker execute() code), to allow immediate
        # shutdown.
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, self._handle_stop_signal_worker)
        if hasattr(signal, 'SIGHUP'):
            signal.signal(signal.SIGHUP, signal.SIG_IGN)

    def _handle_stop_signal_worker(self, sig_num, frame):
        # When draining, the first SIGTERM is ignored so the worker can
        # finish its current task; the main process coordinates the drain
        # using the stop flag. Once the stop flag is set (or draining is
        # disabled), a TERM interrupts the worker immediately.
        if self.drain_timeout is not None and not self.stop_flag.is_set():
            return
        # Raise an interrupt in the subprocess' main loop.
        raise KeyboardInterrupt
