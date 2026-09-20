"""
Migrating from SignedSerializer to SignedEnvelopeSerializer.

Background
----------
The legacy ``SignedSerializer`` signs the pickled payload and *then*
compresses the signed blob. A consumer therefore has to decompress a
message before it can verify the signature -- authentication happens
after decompression, which is the old format's inherent limitation.

``SignedEnvelopeSerializer`` uses a versioned envelope::

    HUEY:<version>:<key-id>:<encoding-flag>:<payload>:<signature>

The HMAC covers the version, key id, encoding flag and the on-the-wire
payload, so consumers authenticate first and only then decompress and
deserialize. It also supports key rotation: producers sign with one
current key id, consumers may trust a set of key ids.

Rolling migration / key rotation order
--------------------------------------
Old consumers cannot read the new envelope format, and new consumers
reject the legacy format unless ``allow_legacy=True`` is passed. The
safe order is therefore:

1. Deploy consumers running ``SignedEnvelopeSerializer`` with
   ``allow_legacy=True`` and *all* keys they should trust (old + new).
2. Switch producers to ``SignedEnvelopeSerializer`` signed with the
   current key id.
3. Once no legacy messages remain in flight, redeploy consumers with
   ``allow_legacy=False`` (the default).
4. To rotate keys: add the new key id to consumers first, switch
   producers to sign with it, then remove the old key id from
   consumers. Messages signed with a removed key id are rejected with
   ``UnknownKeyIdError``.
"""

from huey.serializer import SignedEnvelopeSerializer
from huey.serializer import SignedSerializer


# Step 1: consumer during the migration window. It trusts both the old
# and the new key, and still accepts legacy SignedSerializer messages
# (verified with the key named by legacy_key_id, defaulting to the
# signing key id).
consumer = SignedEnvelopeSerializer(
    keys={
        '2026-09': 'old-secret',
        '2026-10': 'new-secret',
    },
    key_id='2026-10',          # used when this process also produces.
    allow_legacy=True,         # accept legacy SignedSerializer messages.
    legacy_key_id='2026-09',   # key that verifies legacy messages.
    max_decompressed_size=1024 * 1024,
)

# Step 2: producer signs every new message with the current key id.
producer = SignedEnvelopeSerializer(
    keys={'2026-10': 'new-secret'},
    key_id='2026-10',
    compression=True,  # encoding is recorded per-message in the envelope.
)

message = producer.serialize({'task': 'example'})
assert consumer.deserialize(message) == {'task': 'example'}

# Legacy messages still verify during the migration window.
legacy_message = SignedSerializer(secret='old-secret').serialize('old')
assert consumer.deserialize(legacy_message) == 'old'

# Step 3/4: after the migration, consumers drop the legacy format and
# retired key ids. Old messages are then rejected explicitly.
from huey.exceptions import InvalidEnvelopeError, UnknownKeyIdError

final_consumer = SignedEnvelopeSerializer(keys={'2026-10': 'new-secret'})

try:
    final_consumer.deserialize(legacy_message)
except InvalidEnvelopeError:
    pass  # legacy format no longer accepted.

retired = SignedEnvelopeSerializer(
    keys={'2026-09': 'old-secret'}).serialize('stale')
try:
    final_consumer.deserialize(retired)
except UnknownKeyIdError:
    pass  # key id "2026-09" has been retired.
