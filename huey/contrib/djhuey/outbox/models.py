from django.db import models
from django.utils import timezone


class OutboxTask(models.Model):
    """
    Durable record of a Huey task that must be enqueued once the surrounding
    Django transaction commits.

    Delivery is *at-least-once*: a row is only marked as sent after Huey has
    accepted the message, so a crash between the send and the acknowledgement
    results in the same task (with the same task id) being enqueued again.
    """
    PENDING = 'pending'
    CLAIMED = 'claimed'
    SENT = 'sent'
    FAILED = 'failed'
    STATUS_CHOICES = (
        (PENDING, 'pending'),
        (CLAIMED, 'claimed'),
        (SENT, 'sent'),
        (FAILED, 'failed'),
    )

    task_id = models.CharField(max_length=64, unique=True)
    task_name = models.CharField(max_length=255)
    payload = models.BinaryField()
    status = models.CharField(max_length=16, choices=STATUS_CHOICES,
                              default=PENDING, db_index=True)
    available_at = models.DateTimeField(default=timezone.now, db_index=True)
    claimed_at = models.DateTimeField(null=True, blank=True)
    claim_token = models.CharField(max_length=255, null=True, blank=True)
    attempts = models.PositiveIntegerField(default=0)
    last_error = models.TextField(blank=True, default='')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    sent_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        indexes = [models.Index(fields=['status', 'available_at'])]

    def __str__(self):
        return '%s (%s) [%s]' % (self.task_name, self.task_id, self.status)
