"""
Transactional outbox for Huey tasks in Django.

The outbox guarantees *at-least-once* delivery: a task is persisted in the
same database transaction as the business data it belongs to, and a separate
dispatcher (which may run in another process) is responsible for forwarding
committed outbox rows to Huey.  If the process dies between the database
commit and the ``enqueue()`` call, the dispatcher picks the row up on its
next run instead of losing the task.

See the documentation in ``docs/django.rst`` (section "Transactional
outbox") for installation instructions and a description of the failure
semantics.
"""
# NOTE: this package is imported by Django while the app registry is being
# populated, so do NOT import the models (or anything that imports them)
# here.  Import from the submodules instead:
#
#     from huey.contrib.djhuey_outbox.tasks import outbox_task
#     from huey.contrib.djhuey_outbox.dispatcher import OutboxDispatcher
