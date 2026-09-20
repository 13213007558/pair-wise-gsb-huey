import time

from django.core.management.base import BaseCommand
from django.db import DEFAULT_DB_ALIAS

from huey.contrib.djhuey.outbox import dispatcher
from huey.contrib.djhuey.outbox.dispatcher import OutboxDispatcher


class Command(BaseCommand):
    help = (
        'Dispatch pending durable outbox tasks to Huey. Processes one '
        'bounded batch per run (or loops with --loop); safe to run '
        'repeatedly and from multiple processes concurrently.')

    def add_arguments(self, parser):
        parser.add_argument(
            '--using', default=DEFAULT_DB_ALIAS,
            help='Database alias holding the outbox table.')
        parser.add_argument(
            '--batch-size', type=int, default=dispatcher.DEFAULT_BATCH_SIZE,
            help='Maximum number of rows claimed per batch.')
        parser.add_argument(
            '--claim-timeout', type=float,
            default=dispatcher.DEFAULT_CLAIM_TIMEOUT,
            help='Seconds before an abandoned claim may be recovered by '
                 'another dispatcher.')
        parser.add_argument(
            '--max-attempts', type=int,
            default=dispatcher.DEFAULT_MAX_ATTEMPTS,
            help='Failed sends before a row is marked failed.')
        parser.add_argument(
            '--retry-delay', type=float,
            default=dispatcher.DEFAULT_RETRY_DELAY,
            help='Seconds before a failed send becomes eligible for retry.')
        parser.add_argument(
            '--loop', action='store_true',
            help='Keep dispatching instead of exiting after one batch.')
        parser.add_argument(
            '--interval', type=float, default=1.0,
            help='Seconds to sleep between batches in --loop mode.')

    def handle(self, *args, **options):
        outbox_dispatcher = OutboxDispatcher(
            using=options['using'],
            batch_size=options['batch_size'],
            claim_timeout=options['claim_timeout'],
            max_attempts=options['max_attempts'],
            retry_delay=options['retry_delay'])
        if not options['loop']:
            self.stdout.write(str(outbox_dispatcher.dispatch_batch()))
            return
        try:
            while True:
                stats = outbox_dispatcher.dispatch_batch()
                if stats['claimed']:
                    self.stdout.write(str(stats))
                time.sleep(options['interval'])
        except KeyboardInterrupt:
            pass
