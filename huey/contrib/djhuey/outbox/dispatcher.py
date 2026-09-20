import datetime
import logging
import os
import socket
import traceback
import uuid

from django.db import DEFAULT_DB_ALIAS
from django.db.models import Q
from django.utils import timezone

from huey.contrib.djhuey.outbox.models import OutboxTask


logger = logging.getLogger('huey.outbox')

DEFAULT_BATCH_SIZE = 100
DEFAULT_CLAIM_TIMEOUT = 60.0
DEFAULT_MAX_ATTEMPTS = 8
DEFAULT_RETRY_DELAY = 5.0


class OutboxDispatcher(object):
    """
    Moves durable outbox rows into the Huey queue.

    Claiming is a single conditional UPDATE per row, so two dispatchers
    (threads, processes or machines sharing the same database) can never hold
    the same valid claim: exactly one of them will see a rowcount of 1.

    Claims expire after `claim_timeout` seconds. A dispatcher that crashes
    after claiming (or after sending, before acknowledging) leaves its rows
    claimed; another dispatcher reclaims them once the claim expires and
    re-enqueues the *same* task id. Delivery is therefore at-least-once.

    Send failures are recorded on the row (`attempts` and `last_error`)
    and the row is retried after `retry_delay` seconds. After
    `max_attempts` failed sends the row is marked failed and is no longer
    dispatched automatically.
    """
    def __init__(self, huey=None, using=DEFAULT_DB_ALIAS,
                 batch_size=DEFAULT_BATCH_SIZE,
                 claim_timeout=DEFAULT_CLAIM_TIMEOUT,
                 max_attempts=DEFAULT_MAX_ATTEMPTS,
                 retry_delay=DEFAULT_RETRY_DELAY):
        if huey is None:
            from huey.contrib.djhuey import HUEY
            huey = HUEY
        self.huey = huey
        self.using = using
        self.batch_size = batch_size
        self.claim_timeout = claim_timeout
        self.max_attempts = max_attempts
        self.retry_delay = retry_delay
        self.claim_token = '%s:%s:%s' % (
            socket.gethostname(), os.getpid(), uuid.uuid4().hex)

    def dispatch_batch(self):
        """
        Claim up to `batch_size` due rows and enqueue them. Returns a dict
        of counters. Safe to call repeatedly and concurrently.
        """
        rows = self.claim_batch()
        stats = {'claimed': len(rows), 'sent': 0, 'retry': 0, 'failed': 0}
        for row in rows:
            outcome = self.dispatch_one(row)
            stats[outcome] += 1
        return stats

    def _claimable(self, now):
        expires_before = now - datetime.timedelta(seconds=self.claim_timeout)
        return (Q(status=OutboxTask.PENDING, available_at__lte=now) |
                Q(status=OutboxTask.CLAIMED, claimed_at__lte=expires_before))

    def claim_batch(self):
        now = timezone.now()
        manager = OutboxTask.objects.using(self.using)
        claimable = self._claimable(now)
        candidates = list(
            manager.filter(claimable).order_by('pk')
            .values_list('pk', flat=True)[:self.batch_size])
        claimed_pks = []
        for pk in candidates:
            # Atomic compare-and-set: only transitions the row if it is still
            # claimable, so a racing dispatcher loses with rowcount 0.
            updated = manager.filter(claimable, pk=pk).update(
                status=OutboxTask.CLAIMED,
                claimed_at=now,
                claim_token=self.claim_token,
                updated_at=now)
            if updated == 1:
                claimed_pks.append(pk)
        if not claimed_pks:
            return []
        return list(manager.filter(pk__in=claimed_pks,
                                   status=OutboxTask.CLAIMED,
                                   claim_token=self.claim_token)
                    .order_by('pk'))

    def dispatch_one(self, row):
        try:
            task = self.huey.deserialize_task(bytes(row.payload))
            self.huey.enqueue(task)
        except Exception:
            logger.exception('outbox: failed to enqueue %s (%s)',
                             row.task_name, row.task_id)
            return self._record_send_failure(row)
        else:
            # If this acknowledgement is lost (process killed, database
            # error), the row stays claimed and will be reclaimed and
            # re-enqueued with the same task id after the claim expires.
            self._ack_sent(row)
            return 'sent'

    def _ack_sent(self, row):
        now = timezone.now()
        (OutboxTask.objects.using(self.using)
         .filter(pk=row.pk, claim_token=self.claim_token)
         .update(status=OutboxTask.SENT, sent_at=now, updated_at=now,
                 claim_token=None, claimed_at=None))

    def _record_send_failure(self, row):
        now = timezone.now()
        attempts = row.attempts + 1
        if attempts >= self.max_attempts:
            status = OutboxTask.FAILED
            available_at = row.available_at
            outcome = 'failed'
        else:
            status = OutboxTask.PENDING
            available_at = now + datetime.timedelta(seconds=self.retry_delay)
            outcome = 'retry'
        (OutboxTask.objects.using(self.using)
         .filter(pk=row.pk, claim_token=self.claim_token)
         .update(status=status, attempts=attempts,
                 last_error=traceback.format_exc(),
                 available_at=available_at, updated_at=now,
                 claim_token=None, claimed_at=None))
        return outcome


def dispatch_outbox(using=DEFAULT_DB_ALIAS, **kwargs):
    """
    Run a single bounded dispatch batch against the given database alias.
    This is the re-runnable recovery entry-point: it is safe to call it at
    any time, from any number of processes.
    """
    return OutboxDispatcher(using=using, **kwargs).dispatch_batch()
