import time

from django.core.management.base import BaseCommand

from huey.contrib.djhuey_outbox.dispatcher import OutboxDispatcher


class Command(BaseCommand):
    help = (
        'Dispatch committed Huey transactional-outbox tasks to the queue. '
        'Safe to run from several processes concurrently.')

    def add_arguments(self, parser):
        parser.add_argument(
            '--database', default='default',
            help='Django database alias of the outbox table '
                 '(default: "default").')
        parser.add_argument(
            '--batch-size', type=int, default=100,
            help='Maximum tasks claimed and sent per batch (default: 100).')
        parser.add_argument(
            '--claim-timeout', type=float, default=300,
            help='Seconds after which a stale claim by another dispatcher is '
                 'reclaimed (default: 300).')
        parser.add_argument(
            '--max-attempts', type=int, default=5,
            help='Delivery attempts before a task dead-letters as failed '
                 '(default: 5).')
        parser.add_argument(
            '--base-delay', type=float, default=1.0,
            help='Exponential backoff base delay in seconds (default: 1.0).')
        parser.add_argument(
            '--max-delay', type=float, default=3600.0,
            help='Maximum backoff delay in seconds (default: 3600).')
        parser.add_argument(
            '--interval', type=float, default=5.0,
            help='Seconds to sleep between empty batches when looping '
                 '(default: 5.0).')
        parser.add_argument(
            '--once', action='store_true',
            help='Dispatch a single bounded batch and exit instead of '
                 'looping.')

    def handle(self, *args, **options):
        dispatcher = OutboxDispatcher(
            using=options['database'],
            batch_size=options['batch_size'],
            claim_timeout=options['claim_timeout'],
            max_attempts=options['max_attempts'],
            base_delay=options['base_delay'],
            max_delay=options['max_delay'])
        if options['once']:
            count = dispatcher.dispatch_once()
            self.stdout.write('dispatched %s outbox task(s)\n' % count)
            return
        while True:
            count = dispatcher.dispatch_once()
            if count == 0:
                time.sleep(options['interval'])
