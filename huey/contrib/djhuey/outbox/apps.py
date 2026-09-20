from django.apps import AppConfig


class HueyOutboxConfig(AppConfig):
    name = 'huey.contrib.djhuey.outbox'
    label = 'hueyoutbox'
    verbose_name = 'Huey outbox'
    default_auto_field = 'django.db.models.AutoField'
