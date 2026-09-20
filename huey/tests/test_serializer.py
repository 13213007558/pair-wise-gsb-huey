try:
    import gzip
except ImportError:
    gzip = None
import logging
import pickle
import struct
import unittest
try:
    import zlib
except ImportError:
    zlib = None

from huey.exceptions import (
    ConfigurationError,
    EnvelopeError,
    InvalidEnvelopeError,
    PayloadTooLargeError,
    SignatureMismatchError,
    UnknownKeyError)
from huey.serializer import (
    Serializer,
    SignedEnvelopeSerializer,
    SignedSerializer)
from huey.tests.base import BaseTestCase


class TestSerializer(BaseTestCase):
    data = [
        None,
        0, 1,
        b'a' * 1024,
        ['k1', 'k2', 'k3'],
        {'k1': 'v1', 'k2': 'v2', 'k3': 'v3'}]

    def _test_serializer(self, s):
        for item in self.data:
            self.assertEqual(s.deserialize(s.serialize(item)), item)

    def test_serializer(self):
        self._test_serializer(Serializer())

    @unittest.skipIf(gzip is None, 'gzip module not installed')
    def test_serializer_gzip(self):
        self._test_serializer(Serializer(compression=True))

    @unittest.skipIf(zlib is None, 'zlib module not installed')
    def test_serializer_zlib(self):
        self._test_serializer(Serializer(compression=True, use_zlib=True))

    @unittest.skipIf(zlib is None, 'zlib module not installed')
    @unittest.skipIf(gzip is None, 'gzip module not installed')
    def test_mismatched_compression(self):
        for use_zlib in (False, True):
            s = Serializer()
            scomp = Serializer(compression=True, use_zlib=use_zlib)
            for item in self.data:
                self.assertEqual(scomp.deserialize(s.serialize(item)), item)


KEYS_V1 = {'v1': 'secret-v1'}
KEYS_V2 = {'v2': 'secret-v2'}
KEYS_BOTH = {'v1': 'secret-v1', 'v2': 'secret-v2'}


class RecordingSerializer(SignedEnvelopeSerializer):
    """Records the order of decompress/deserialize calls."""

    def __init__(self, *args, **kwargs):
        self.calls = []
        super(RecordingSerializer, self).__init__(*args, **kwargs)

    def _decompress(self, encoding, data):
        self.calls.append('decompress')
        return super(RecordingSerializer, self)._decompress(encoding, data)

    def _deserialize(self, data):
        self.calls.append('deserialize')
        return super(RecordingSerializer, self)._deserialize(data)


class TestSignedEnvelopeSerializer(BaseTestCase):
    data = [
        None,
        0, 1,
        b'a' * 1024,
        ['k1', 'k2', 'k3'],
        {'k1': 'v1', 'k2': 'v2', 'k3': 'v3'}]

    def producer(self, keys=KEYS_BOTH, current='v2', **kwargs):
        return SignedEnvelopeSerializer(keys=keys, current_key_id=current,
                                        **kwargs)

    def consumer(self, keys=KEYS_BOTH, **kwargs):
        return SignedEnvelopeSerializer(keys=keys, **kwargs)

    def _test_roundtrip(self, producer, consumer):
        for item in self.data:
            self.assertEqual(consumer.deserialize(producer.serialize(item)),
                             item)

    def test_roundtrip_uncompressed(self):
        self._test_roundtrip(self.producer(), self.consumer())

    @unittest.skipIf(gzip is None, 'gzip module not installed')
    def test_roundtrip_gzip(self):
        self._test_roundtrip(self.producer(compression=True),
                             self.consumer())

    @unittest.skipIf(zlib is None, 'zlib module not installed')
    def test_roundtrip_zlib(self):
        self._test_roundtrip(
            self.producer(compression=True, use_zlib=True), self.consumer())

    def test_consumer_uses_envelope_encoding_flag(self):
        # The consumer decodes using the flag in the envelope, not its own
        # compression settings, and never tries other encodings.
        for use_zlib in (False, True):
            producer = self.producer(compression=True, use_zlib=use_zlib)
            consumer = self.consumer(compression=True, use_zlib=not use_zlib)
            self._test_roundtrip(producer, consumer)

    def test_consumer_without_current_key_cannot_serialize(self):
        consumer = self.consumer()
        self.assertRaises(ConfigurationError, consumer.serialize, 'x')
        producer = self.producer()
        self.assertEqual(consumer.deserialize(producer.serialize('x')), 'x')

    def test_configuration_errors(self):
        self.assertRaises(ConfigurationError, SignedEnvelopeSerializer)
        self.assertRaises(ConfigurationError, SignedEnvelopeSerializer,
                          keys={})
        self.assertRaises(ConfigurationError, SignedEnvelopeSerializer,
                          keys=KEYS_V1, current_key_id='v9')
        self.assertRaises(ConfigurationError, SignedEnvelopeSerializer,
                          keys=KEYS_V1, allow_legacy=True)

    def test_key_rotation(self):
        # Phase 1: old producer signs with v1, old consumer knows v1.
        old_producer = self.producer(KEYS_V1, 'v1')
        old_consumer = self.consumer(KEYS_V1)
        msg_v1 = old_producer.serialize('task-v1')
        self.assertEqual(old_consumer.deserialize(msg_v1), 'task-v1')

        # Phase 2: consumer updated to accept both keys; old producer
        # still works.
        consumer = self.consumer(KEYS_BOTH)
        self.assertEqual(consumer.deserialize(msg_v1), 'task-v1')

        # Phase 3: producer switches to signing with v2.
        new_producer = self.producer(KEYS_BOTH, 'v2')
        msg_v2 = new_producer.serialize('task-v2')
        self.assertEqual(consumer.deserialize(msg_v2), 'task-v2')

        # An old consumer that only knows v1 rejects v2 messages.
        self.assertRaises(UnknownKeyError, old_consumer.deserialize, msg_v2)

        # Phase 4: v1 removed -- old messages are explicitly rejected.
        final_consumer = self.consumer(KEYS_V2)
        self.assertEqual(final_consumer.deserialize(msg_v2), 'task-v2')
        self.assertRaises(UnknownKeyError, final_consumer.deserialize, msg_v1)
        final_consumer = self.consumer(KEYS_V2)
        self.assertEqual(final_consumer.deserialize(msg_v2), 'task-v2')
        self.assertRaises(UnknownKeyError, final_consumer.deserialize, msg_v1)

    def _flip(self, data, offset):
        mutated = bytearray(data)
        mutated[offset] ^= 0xff
        return bytes(mutated)

    def test_tampered_version_rejected(self):
        consumer = self.consumer()
        msg = self.producer().serialize('task')
        # Byte 4 is the version field; it is covered by the signature.
        self.assertRaises(SignatureMismatchError, consumer.deserialize,
                          self._flip(msg, 4))

    def test_tampered_key_id_rejected(self):
        consumer = self.consumer()
        msg = self.producer(KEYS_BOTH, 'v1').serialize('task')
        # Byte 6 is the first key-id byte; v1 -> another known id (v2) is
        # caught by the signature...
        mutated = bytearray(msg)
        mutated[7] = ord('2')
        self.assertRaises(SignatureMismatchError, consumer.deserialize,
                          bytes(mutated))
        # ...while an unknown key id is rejected as such.
        mutated = bytearray(msg)
        mutated[7] = ord('9')
        self.assertRaises(UnknownKeyError, consumer.deserialize,
                          bytes(mutated))

    def test_tampered_encoding_flag_rejected(self):
        consumer = self.consumer()
        msg = self.producer(compression=True).serialize('task')
        # key id v2 is 2 bytes: magic(4) ver(1) len(1) kid(2) -> offset 8.
        self.assertRaises(SignatureMismatchError, consumer.deserialize,
                          self._flip(msg, 8))

    def test_tampered_payload_rejected(self):
        consumer = self.consumer()
        msg = self.producer().serialize('task')
        self.assertRaises(SignatureMismatchError, consumer.deserialize,
                          self._flip(msg, 12))

    def test_tampered_signature_rejected(self):
        consumer = self.consumer()
        msg = self.producer().serialize('task')
        self.assertRaises(SignatureMismatchError, consumer.deserialize,
                          self._flip(msg, len(msg) - 1))
        self.assertRaises(SignatureMismatchError, consumer.deserialize,
                          self._flip(msg, len(msg) - 32))

    def _signed_envelope(self, serializer, version, encoding, payload):
        key_id = serializer.current_key_id
        header = struct.pack('!4sBB', b'HUEY', version, len(key_id))
        signed = header + key_id + struct.pack('!B', encoding) + payload
        return signed + serializer._sign(serializer._keys[key_id], signed)

    def test_unsupported_version_and_encoding_rejected(self):
        # Even with a valid signature, unknown versions/encodings are
        # rejected rather than being probed with every decoder.
        producer = self.producer()
        consumer = self.consumer()
        payload = producer._serialize('task')
        msg = self._signed_envelope(producer, 2, 0, payload)
        self.assertRaises(InvalidEnvelopeError, consumer.deserialize, msg)
        msg = self._signed_envelope(producer, 1, 99, payload)
        self.assertRaises(InvalidEnvelopeError, consumer.deserialize, msg)

    def test_invalid_inputs_rejected(self):
        consumer = self.consumer()
        bad_inputs = [
            b'',
            b'garbage',
            pickle.dumps('plain pickle is never accepted'),
            b'HUEY',
            b'HUEY\x01\x02v2',  # truncated envelope
            b'HUEY\x01\x00' + b'x' * 40,  # empty key id
            b'HUEY\x01\xfev' + b'x' * 40,  # key id overruns message
        ]
        for bad in bad_inputs:
            self.assertRaises(InvalidEnvelopeError, consumer.deserialize,
                              bad)

    def test_errors_are_distinguishable(self):
        for exc_class in (InvalidEnvelopeError, UnknownKeyError,
                          SignatureMismatchError, PayloadTooLargeError):
            self.assertTrue(issubclass(exc_class, EnvelopeError))
        self.assertRaises(UnknownKeyError,
                          self.consumer(KEYS_V2).deserialize,
                          self.producer(KEYS_V1, 'v1').serialize('x'))
        self.assertRaises(SignatureMismatchError,
                          self.consumer({'v1': 'wrong-secret'}).deserialize,
                          self.producer(KEYS_V1, 'v1').serialize('x'))
        self.assertRaises(SignatureMismatchError,
                          self.consumer({'v1': 'wrong-secret'}).deserialize,
                          self.producer(KEYS_V1, 'v1').serialize('x'))

    def _bomb_message(self, use_zlib, size=4 * 1024 * 1024):
        # A small, validly-signed message that decompresses to ~4MB.
        producer = self.producer(compression=True, use_zlib=use_zlib)
        return producer.serialize(b'A' * size)

    @unittest.skipIf(gzip is None, 'gzip module not installed')
    def test_gzip_bomb_rejected(self):
        consumer = self.consumer(max_decompressed_size=1024)
        self.assertRaises(PayloadTooLargeError, consumer.deserialize,
                          self._bomb_message(False))

    @unittest.skipIf(zlib is None, 'zlib module not installed')
    def test_zlib_bomb_rejected(self):
        consumer = self.consumer(max_decompressed_size=1024)
        self.assertRaises(PayloadTooLargeError, consumer.deserialize,
                          self._bomb_message(True))

    def test_oversized_uncompressed_rejected(self):
        consumer = self.consumer(max_decompressed_size=1024)
        msg = self.producer().serialize(b'A' * 4096)
        self.assertRaises(PayloadTooLargeError, consumer.deserialize, msg)

    def test_within_limit_accepted(self):
        consumer = self.consumer(max_decompressed_size=4096)
        for use_zlib in (False, True):
            producer = self.producer(compression=True, use_zlib=use_zlib)
            msg = producer.serialize(b'A' * 1024)
            self.assertEqual(consumer.deserialize(msg), b'A' * 1024)

    def test_auth_failure_runs_no_decompress_or_deserialize(self):
        consumer = RecordingSerializer(keys=KEYS_BOTH)
        producer = self.producer(compression=True)

        # Bad signature: neither decompress nor deserialize may run.
        tampered = self._flip(producer.serialize('task'), 12)
        self.assertRaises(SignatureMismatchError, consumer.deserialize,
                          tampered)
        self.assertEqual(consumer.calls, [])

        # Unknown key id: same guarantee.
        consumer = RecordingSerializer(keys=KEYS_V2)
        msg_v1 = self.producer(KEYS_V1, 'v1', compression=True)
        self.assertRaises(UnknownKeyError, consumer.deserialize,
                          msg_v1.serialize('task'))
        self.assertEqual(consumer.calls, [])

        # Invalid envelope: same guarantee.
        consumer = RecordingSerializer(keys=KEYS_BOTH)
        self.assertRaises(InvalidEnvelopeError, consumer.deserialize,
                          b'not an envelope')
        self.assertEqual(consumer.calls, [])

        # Oversized payload: deserialize must not run after decompress.
        consumer = RecordingSerializer(keys=KEYS_BOTH,
                                       max_decompressed_size=8)
        self.assertRaises(PayloadTooLargeError, consumer.deserialize,
                          producer.serialize(b'A' * 4096))
        self.assertEqual(consumer.calls, ['decompress'])

        # Valid message: decompress runs before deserialize, in that order.
        consumer = RecordingSerializer(keys=KEYS_BOTH)
        self.assertEqual(consumer.deserialize(producer.serialize('ok')),
                         'ok')
        self.assertEqual(consumer.calls, ['decompress', 'deserialize'])

    def test_legacy_requires_explicit_opt_in(self):
        legacy = SignedSerializer(secret='old-secret', compression=True)
        msg = legacy.serialize('legacy-task')

        # By default the legacy format is rejected outright.
        consumer = self.consumer(compression=True)
        self.assertRaises(InvalidEnvelopeError, consumer.deserialize, msg)

        # With the migration option enabled it is accepted.
        consumer = self.consumer(compression=True, allow_legacy=True,
                                 legacy_secret='old-secret')
        self.assertEqual(consumer.deserialize(msg), 'legacy-task')

        # The legacy branch still authenticates (with the old scheme): a
        # legacy message with a bad signature is rejected.
        raw = gzip.decompress(msg)
        raw_bad = raw[:-1] + (b'0' if raw[-1:] != b'0' else b'1')
        self.assertRaises(ValueError, consumer.deserialize,
                          gzip.compress(raw_bad))

        # Documented legacy limitation: because the old format
        # authenticates after decompression, corrupting the compressed
        # data surfaces a decompression error before any authentication
        # happens.
        self.assertRaises(IOError, consumer.deserialize,
                          self._flip(msg, len(msg) - 1))
        bad_consumer = self.consumer(compression=True, allow_legacy=True,
                                     legacy_secret='wrong-secret')
        self.assertRaises(ValueError, bad_consumer.deserialize, msg)

    def test_legacy_branch_cannot_bypass_new_format(self):
        # allow_legacy must not weaken verification of new envelopes.
        consumer = self.consumer(allow_legacy=True,
                                 legacy_secret='old-secret')
        msg = self.producer().serialize('task')
        self.assertRaises(SignatureMismatchError, consumer.deserialize,
                          self._flip(msg, 12))
        self.assertRaises(UnknownKeyError,
                          self.consumer(KEYS_V2, allow_legacy=True,
                                        legacy_secret='old-secret')
                          .deserialize,
                          self.producer(KEYS_V1, 'v1').serialize('task'))

    def test_errors_and_logs_do_not_leak_secrets_or_payload(self):
        secret = 'super-secret-signing-key'
        marker = 'TOPSECRET-task-body'
        producer = SignedEnvelopeSerializer(keys={'v1': secret},
                                            current_key_id='v1')
        consumer = SignedEnvelopeSerializer(
            keys={'v1': secret}, max_decompressed_size=8)

        log_records = []

        class Handler(logging.Handler):
            def emit(self, record):
                log_records.append(record.getMessage())

        handler = Handler()
        logger = logging.getLogger('huey.serializer')
        logger.addHandler(handler)
        try:
            failures = [
                self._flip(producer.serialize(marker), 12),
                producer.serialize(marker)[:20],  # truncated
                producer.serialize(marker),  # over the size limit
            ]
            for bad in failures:
                try:
                    consumer.deserialize(bad)
                except EnvelopeError as exc:
                    self.assertNotIn(secret, str(exc))
                    self.assertNotIn(marker, str(exc))
                else:
                    self.fail('expected EnvelopeError')
            unknown = SignedEnvelopeSerializer(keys={'v9': secret})
            try:
                unknown.deserialize(producer.serialize(marker))
            except UnknownKeyError as exc:
                self.assertNotIn(secret, str(exc))
                self.assertNotIn(marker, str(exc))
        finally:
            logger.removeHandler(handler)

        for message in log_records:
            self.assertNotIn(secret, message)
            self.assertNotIn(marker, message)
