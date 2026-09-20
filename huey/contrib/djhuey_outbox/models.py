from django.db import models
from django.utils import timezone


class OutboxTask(models.Model):
    """A serialized Huey task that is pending delivery to the queue."""

    PENDING = 'pending'
    IN_PROGRESS = 'in_progress'  # Claimed by a dispatcher.
    SENT = 'sent'                # Confirmed delivered to the Huey queue.
    FAILED = 'failed'            # Retries exhausted (dead-letter).
    STATUS_CHOICES = (
        (PENDING, 'Pending'),
        (IN_PROGRESS, 'In progress'),
        (SENT, 'Sent'),
        (FAILED, 'Failed'),
    )

    # Stable Huey task id, reused on every delivery attempt.  Huey task ids
    # are hex UUID strings, but the field is left wide enough for custom
    # ``Task.create_id()`` implementations.
    task_id = models.CharField(max_length=128, unique=True)
    task_name = models.CharField(max_length=255)
    # Bytes produced by ``Huey.serialize_task()``.
    message = models.BinaryField()

    status = models.CharField(max_length=16, choices=STATUS_CHOICES,
                              default=PENDING, db_index=True)
    attempts = models.PositiveIntegerField(default=0)
    next_attempt_at = models.DateTimeField(default=timezone.now, db_index=True)

    claimed_by = models.CharField(max_length=128, blank=True, default='')
    claimed_at = models.DateTimeField(null=True, blank=True)
    sent_at = models.DateTimeField(null=True, blank=True)
    last_error = models.TextField(blank=True, default='')

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = 'djhuey_outbox'
        indexes = (
            models.Index(fields=('status', 'next_attempt_at')),
        )

    def __str__(self):
        return 'OutboxTask %s (%s)' % (self.task_name, self.task_id)

    @classmethod
    def from_task(cls, task, message=None, huey=None):
        """Build (but do not save) an outbox row for a Huey ``Task``.

        The caller must persist the row inside the business transaction.
        """
        if message is None:
            if huey is None:
                raise ValueError('huey is required when message is not given')
            message = huey.serialize_task(task)
        return cls(
            task_id=task.id,
            task_name=type(task).__name__,
            message=bytes(message))
