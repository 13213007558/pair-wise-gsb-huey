from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name='OutboxTask',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True,
                                         serialize=False, verbose_name='ID')),
                ('task_id', models.CharField(max_length=128, unique=True)),
                ('task_name', models.CharField(max_length=255)),
                ('message', models.BinaryField()),
                ('status', models.CharField(
                    choices=[('pending', 'Pending'),
                             ('in_progress', 'In progress'),
                             ('sent', 'Sent'),
                             ('failed', 'Failed')],
                    db_index=True, default='pending', max_length=16)),
                ('attempts', models.PositiveIntegerField(default=0)),
                ('next_attempt_at', models.DateTimeField(
                    db_index=True,
                    default=django.utils.timezone.now)),
                ('claimed_by', models.CharField(
                    blank=True, default='', max_length=128)),
                ('claimed_at', models.DateTimeField(
                    blank=True, null=True)),
                ('sent_at', models.DateTimeField(blank=True, null=True)),
                ('last_error', models.TextField(blank=True, default='')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
            ],
            options={
                'indexes': [
                    models.Index(fields=['status', 'next_attempt_at'],
                                 name='djhuey_outb_status_ce7d7a_idx'),
                ],
            },
        ),
    ]
