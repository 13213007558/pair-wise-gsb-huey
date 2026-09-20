"""
Forward committed outbox rows to Huey with at-least-once semantics.

Delivery protocol for one outbox row::

    1. CLAIM   atomically (conditional UPDATE): pending OR a stale claim of
               another dispatcher -> in_progress owned by us.
    2. SEND    enqueue the (stable-id) serialized task into Huey.
    3. CONFIRM mark the row ``sent``.

Crash windows:

* Crash before/during CLAIM  -> the row stays ``pending`` and is retried.
* Crash after SEND, before CONFIRM -> the row is claimed again after the
  claim timeout and the SAME message (same Huey task id) is enqueued again.
  This is the unavoidable duplicate-delivery window: Huey consumers must be
  idempotent.  Delivery is at-least-once, NOT exactly-once.
* SEND raises -> the error is recorded on the row (``last_error``), the
  attempt is counted and the row is rescheduled with exponential backoff.
  Rows exceeding ``max_attempts`` are moved to the ``failed`` dead-letter
  status and stop being dispatched.
"""
import logging
import socket
import time
import uuid

from django.db import OperationalError
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from huey.contrib.djhuey_outbox.models import OutboxTask


logger = logging.getLogger('huey.outbox')

# Database error texts that indicate a transient lock/contention instead of a
# real failure (SQLite in particular raises OperationalError under concurrent
# writers; PostgreSQL serializable/deadlock errors are similar).
_RETRY_LOCK_ERRORS = ('database is locked', 'database table is locked',
                      'could not obtain lock', 'deadlock detected',
                      'serialization failure')


def _is_lock_error(exc):
    message = str(exc).lower()
    return any(text in message for text in _RETRY_LOCK_ERRORS)


def default_owner_id():
    return '%s:%s' % (socket.gethostname(), uuid.uuid4().hex[:8])


class OutboxDispatcher(object):
    """Claim outbox rows and deliver them to a Huey queue.

    :param huey: Huey instance (defaults to ``huey.contrib.djhuey.HUEY``).
    :param using: Django database alias of the outbox table.
    :param batch_size: maximum rows claimed and sent per iteration.
    :param claim_timeout: seconds after which another dispatcher's claim is
        considered dead and the row becomes reclaimable.
    :param max_attempts: total delivery attempts before a row dead-letters.
    :param base_delay, max_delay: exponential backoff bounds in seconds.
    :param owner: identity recorded in ``claimed_by`` (for diagnostics).
    """

    def __init__(self, huey=None, using='default', batch_size=100,
                 claim_timeout=300, max_attempts=5, base_delay=1.0,
                 max_delay=3600.0, owner=None, lock_retries=5,
                 lock_retry_delay=0.05):
        self._huey = huey
        self.using = using
        self.batch_size = batch_size
        self.claim_timeout = claim_timeout
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.owner = owner or default_owner_id()
        self.lock_retries = lock_retries
        self.lock_retry_delay = lock_retry_delay

    @property
    def huey(self):
        if self._huey is None:
            from huey.contrib.djhuey import HUEY
            self._huey = HUEY
        return self._huey

    # -- public API -------------------------------------------------------

    def dispatch_once(self):
        """Run at most ``batch_size`` deliveries; return the number sent.

        Re-claims in a loop only when a concurrent dispatcher raced ahead on
        a row we selected, so the total work per call stays bounded by
        ``batch_size`` successful claims.  Rows whose send fails are moved to
        a future ``next_attempt_at`` and are not retried in the same call.
        """
        sent = 0
        budget = self.batch_size
        processed_ids = set()
        while sent < self.batch_size:
            claimed = self.claim_batch(limit=budget,
                                       exclude_ids=processed_ids)
            if not claimed:
                break
            for outbox_task in claimed:
                processed_ids.add(outbox_task.pk)
                sent += self._deliver(outbox_task)
            # Rows that failed to send are rescheduled with a future
            # next_attempt_at, so the following claim only returns genuinely
            # due rows (typically rows lost to a racing dispatcher).  The
            # budget keeps the total work per call bounded by batch_size.
            budget -= len(claimed)
        return sent

    def run_forever(self, interval=5.0):
        """Repeatedly run :meth:`dispatch_once`, sleeping ``interval`` s."""
        while True:
            count = self.dispatch_once()
            if count == 0:
                time.sleep(interval)

    def _with_lock_retry(self, operation):
        """Run ``operation``, retrying transient database lock errors."""
        for attempt in range(self.lock_retries + 1):
            try:
                return operation()
            except OperationalError as exc:
                if not _is_lock_error(exc) or attempt == self.lock_retries:
                    raise
                time.sleep(self.lock_retry_delay * (attempt + 1))

    # -- claiming ---------------------------------------------------------

    def claim_batch(self, limit=None, exclude_ids=None):
        """Atomically claim up to ``limit`` due rows in one transaction.

        Returns the list of claimed :class:`OutboxTask` rows.  The claim is
        committed BEFORE any network I/O, so a process crash leaves a
        visible (timed-out) claim rather than a lost task.
        """
        limit = self.batch_size if limit is None else limit
        now = timezone.now()
        stale_before = now - timezone.timedelta(seconds=self.claim_timeout)
        qs = OutboxTask.objects.using(self.using)

        def claim():
            with transaction.atomic(using=self.using):
                # Pick candidates first.  This SELECT does not lock
                # anything; the conditional UPDATE below is what makes the
                # claim atomic (it is a single statement, so it works on
                # SQLite, where writes are serialized, as well as on
                # PostgreSQL/MySQL).
                candidate_ids = list(
                    qs.filter(self._due_q(stale_before))
                      .exclude(pk__in=exclude_ids or ())
                      .order_by('next_attempt_at', 'id')
                      [:limit]
                      .values_list('id', flat=True))
                if not candidate_ids:
                    return []

                # Conditional UPDATE is the actual claim.  A concurrent
                # dispatcher that selected the same rows matches none of
                # them: their status is now in_progress and claimed_at is
                # fresh.
                claimed = qs.filter(pk__in=candidate_ids).filter(
                    self._due_q(stale_before)).exclude(
                    pk__in=exclude_ids or ()).update(
                        status=OutboxTask.IN_PROGRESS,
                        claimed_by=self.owner,
                        claimed_at=now)
                if claimed == 0:
                    return []
                rows = list(qs.filter(pk__in=candidate_ids,
                                      claimed_by=self.owner,
                                      claimed_at=now))
                return rows[:claimed]

        return self._with_lock_retry(claim)

    @staticmethod
    def _due_q(stale_before):
        return (
            Q(status=OutboxTask.PENDING,
            next_attempt_at__lte=timezone.now()) |
            Q(status=OutboxTask.IN_PROGRESS,
              claimed_at__lte=stale_before)
        )

    # -- delivery ---------------------------------------------------------

    def _deliver(self, outbox_task):
        """Send one claimed row.  Returns 1 if sent, 0 if the claim raced."""
        # Re-read the row; a concurrent confirmation could have finished it.
        qs = OutboxTask.objects.using(self.using)
        fresh = qs.filter(pk=outbox_task.pk,
                          status=OutboxTask.IN_PROGRESS,
                          claimed_by=self.owner).first()
        if fresh is None:
            return 0
        outbox_task = fresh

        task = self.deserialize(outbox_task)
        try:
            self.send(outbox_task, task)
        except Exception as exc:
            logger.warning('outbox task %s delivery attempt %s failed: %s',
                           outbox_task.task_id, outbox_task.attempts + 1, exc)
            self._record_failure(outbox_task, exc)
            return 0

        self.mark_sent(outbox_task)
        logger.info('outbox task %s delivered', outbox_task.task_id)
        return 1

    def deserialize(self, outbox_task):
        return self.huey.deserialize_task(bytes(outbox_task.message))

    # -- hooks / fault-injection points ----------------------------------

    def send(self, outbox_task, task):
        """Put the serialized message into Huey.

        Uses the storage directly (instead of ``Huey.enqueue``) so that no
        ``SIGNAL_ENQUEUED`` side effects or immediate-mode execution occur in
        the dispatcher process.  The stable task id is preserved.
        """
        self.huey.storage.enqueue(bytes(outbox_task.message), task.priority)

    def mark_sent(self, outbox_task):
        """CONFIRM step.  Separate method so tests can crash here."""
        def confirm():
            OutboxTask.objects.using(self.using).filter(
                pk=outbox_task.pk).update(
                    status=OutboxTask.SENT,
                    attempts=outbox_task.attempts + 1,
                    sent_at=timezone.now(),
                    last_error='')
        self._with_lock_retry(confirm)

    def _record_failure(self, outbox_task, exc):
        attempts = outbox_task.attempts + 1
        error = '%s: %s' % (type(exc).__name__, exc)
        if attempts >= self.max_attempts:
            status = OutboxTask.FAILED
            next_at = outbox_task.next_attempt_at
        else:
            status = OutboxTask.PENDING
            delay = min(self.max_delay,
                        self.base_delay * (2 ** (attempts - 1)))
            next_at = timezone.now() + timezone.timedelta(seconds=delay)
        def reschedule():
            OutboxTask.objects.using(self.using).filter(
                pk=outbox_task.pk).update(
                    status=status,
                    attempts=attempts,
                    next_attempt_at=next_at,
                    claimed_by='',
                    claimed_at=None,
                    last_error=error[:4096])
        self._with_lock_retry(reschedule)
