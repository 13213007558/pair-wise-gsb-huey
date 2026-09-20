from django.apps import AppConfig


class DjhueyOutboxConfig(AppConfig):
    name = 'huey.contrib.djhuey_outbox'
    label = 'djhuey_outbox'
    default_auto_field = 'django.db.models.BigAutoField'
    verbose_name = 'Huey transactional outbox'
