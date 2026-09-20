"""
Migrating from SignedSerializer to SignedEnvelopeSerializer, and rotating
signing keys without dropping messages.

SignedEnvelopeSerializer signs a versioned envelope that covers the
version, key id, encoding flag and the on-the-wire payload, and only
decompresses / unpickles *after* the signature verifies. The old
SignedSerializer authenticates after decompression, so its format is
only accepted when explicitly enabled via allow_legacy=True.

Run directly to see each migration phase:

    python examples/serializer_migration.py
"""

from huey.exceptions import UnknownKeyError
from huey.serializer import SignedEnvelopeSerializer, SignedSerializer


OLD_SECRET = 'old-shared-secret'
KEY_V1 = 'rotating-secret-v1'
KEY_V2 = 'rotating-secret-v2'


def phase(title):
    print('\n=== %s ===' % title)


def main():
    phase('0. Before migration: old producer / old consumer')
    old_producer = SignedSerializer(secret=OLD_SECRET, compression=True)
    old_consumer = SignedSerializer(secret=OLD_SECRET, compression=True)
    legacy_msg = old_producer.serialize('legacy-task')
    assert old_consumer.deserialize(legacy_msg) == 'legacy-task'
    print('legacy format round-trips')

    phase('1. Upgrade consumers first (accept new envelope AND legacy)')
    # allow_legacy keeps old producers working while producers migrate.
    # NOTE: the legacy branch authenticates after decompression (the old
    # format's inherent limitation), so keep it enabled only during the
    # migration window.
    consumer = SignedEnvelopeSerializer(
        keys={'v1': KEY_V1},
        compression=True,
        max_decompressed_size=1024 * 1024,
        allow_legacy=True,
        legacy_secret=OLD_SECRET)
    assert consumer.deserialize(legacy_msg) == 'legacy-task'
    print('legacy message accepted during migration window')

    phase('2. Switch producers to the new envelope (key id v1)')
    producer = SignedEnvelopeSerializer(
        keys={'v1': KEY_V1}, current_key_id='v1', compression=True)
    msg_v1 = producer.serialize('task-v1')
    assert consumer.deserialize(msg_v1) == 'task-v1'
    print('new envelope accepted')

    phase('3. Disable legacy once all producers emit the new envelope')
    consumer = SignedEnvelopeSerializer(
        keys={'v1': KEY_V1},
        compression=True,
        max_decompressed_size=1024 * 1024)
    assert consumer.deserialize(msg_v1) == 'task-v1'
    try:
        consumer.deserialize(legacy_msg)
    except Exception as exc:
        print('legacy message now rejected: %s' % type(exc).__name__)

    phase('4. Rotate keys: consumers accept v1+v2, producers move to v2')
    consumer = SignedEnvelopeSerializer(
        keys={'v1': KEY_V1, 'v2': KEY_V2}, compression=True)
    assert consumer.deserialize(msg_v1) == 'task-v1'  # old key still ok
    producer = SignedEnvelopeSerializer(
        keys={'v1': KEY_V1, 'v2': KEY_V2},
        current_key_id='v2', compression=True)
    msg_v2 = producer.serialize('task-v2')
    assert consumer.deserialize(msg_v2) == 'task-v2'
    print('messages signed with v1 and v2 both accepted')

    phase('5. Remove the old key: v1 messages are explicitly rejected')
    consumer = SignedEnvelopeSerializer(
        keys={'v2': KEY_V2}, compression=True)
    assert consumer.deserialize(msg_v2) == 'task-v2'
    try:
        consumer.deserialize(msg_v1)
    except UnknownKeyError:
        print('v1 message rejected: UnknownKeyError')

    print('\nMigration complete.')


if __name__ == '__main__':
    main()
