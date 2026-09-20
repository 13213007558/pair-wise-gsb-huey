from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='OutboxTask',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True,
                                        serialize=False, verbose_name='ID')),
                ('task_id', models.CharField(max_length=64, unique=True)),
                ('task_name', models.CharField(max_length=255)),
                ('payload', models.BinaryField()),
                ('status', models.CharField(
                    choices=[('pending', 'pending'), ('claimed', 'claimed'),
                             ('sent', 'sent'), ('failed', 'failed')],
                    db_index=True, default='pending', max_length=16)),
                ('available_at', models.DateTimeField(
                    db_index=True, default=django.utils.timezone.now)),
                ('claimed_at', models.DateTimeField(blank=True, null=True)),
                ('claim_token', models.CharField(blank=True, max_length=255,
                                                 null=True)),
                ('attempts', models.PositiveIntegerField(default=0)),
                ('last_error', models.TextField(blank=True, default='')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('sent_at', models.DateTimeField(blank=True, null=True)),
            ],
        ),
        migrations.AddIndex(
            model_name='outboxtask',
            index=models.Index(fields=['status', 'available_at'],
                               name='hueyoutbox__status_116ace_idx'),
        ),
    ]
