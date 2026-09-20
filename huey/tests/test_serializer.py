try:
    import gzip
except ImportError:
    gzip = None
import pickle
import unittest
try:
    import zlib
except ImportError:
    zlib = None

from huey.exceptions import BadSignatureError
from huey.exceptions import ConfigurationError
from huey.exceptions import InvalidEnvelopeError
from huey.exceptions import PayloadTooLargeError
from huey.exceptions import SerializerError
from huey.exceptions import UnknownKeyIdError
from huey.serializer import Serializer
from huey.serializer import SignedSerializer
from huey.serializer import SignedEnvelopeSerializer
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


class TestSignedEnvelopeSerializer(BaseTestCase):
    keys = {'key-a': 'secret-a', 'key-b': 'secret-b'}
    data = [
        None,
        0, 1,
        b'a' * 1024,
        ['k1', 'k2', 'k3'],
        {'k1': 'v1', 'k2': 'v2', 'k3': 'v3'}]

    def get_serializer(self, **kwargs):
        params = {'keys': dict(self.keys), 'key_id': 'key-a'}
        params.update(kwargs)
        return SignedEnvelopeSerializer(**params)

    def make_envelope(self, version=b'2', key_id=b'key-a', flag=b'n',
                      payload=None, signer=None):
        signer = signer or self.get_serializer()
        if payload is None:
            payload = pickle.dumps({'n': 1}, pickle.HIGHEST_PROTOCOL)
        signed = b':'.join((b'HUEY', version, key_id, flag)) + b':' + payload
        return signed + b':' + signer._signature(signed, key_id)

    def tamper_signature(self, message):
        replacement = b'0' if message[-1:] != b'0' else b'1'
        return message[:-1] + replacement

    def test_roundtrip_all_encodings(self):
        consumer = self.get_serializer()
        producers = [self.get_serializer()]
        if gzip is not None:
            producers.append(self.get_serializer(compression=True))
        if zlib is not None:
            producers.append(self.get_serializer(compression=True,
                                                 use_zlib=True))
        for producer in producers:
            for item in self.data:
                self.assertEqual(consumer.deserialize(producer.serialize(item)),
                                 item)

    def test_secret_convenience_constructor(self):
        producer = SignedEnvelopeSerializer(secret='s3cret', key_id='main')
        consumer = SignedEnvelopeSerializer(keys={'main': 's3cret'})
        self.assertEqual(consumer.deserialize(producer.serialize('x')), 'x')

    def test_configuration_errors(self):
        self.assertRaises(ConfigurationError, SignedEnvelopeSerializer)
        self.assertRaises(ConfigurationError, SignedEnvelopeSerializer,
                          keys={'key-a': 'secret-a'}, key_id='key-z')
        self.assertRaises(ConfigurationError, SignedEnvelopeSerializer,
                          keys=self.keys)
        self.assertRaises(ConfigurationError, SignedEnvelopeSerializer,
                          secret='secret-a')
        self.assertRaises(ConfigurationError, SignedEnvelopeSerializer,
                          keys={'key-a': 'secret-a'}, secret='secret-a',
                          key_id='key-a')
        self.assertRaises(ConfigurationError, SignedEnvelopeSerializer,
                          keys={'bad key!': 'secret-a'})
        self.assertRaises(ConfigurationError, SignedEnvelopeSerializer,
                          keys={'key-a': 'secret-a'}, allow_legacy=True,
                          legacy_key_id='key-z')
        self.assertRaises(ConfigurationError, SignedEnvelopeSerializer,
                          keys={'key-a': 'secret-a'}, salt='')

    def test_key_rotation_order(self):
        # Phase 1: producers and consumers both use key-a.
        producer = self.get_serializer()
        consumer = SignedEnvelopeSerializer(keys={'key-a': 'secret-a'})
        msg_a = producer.serialize({'n': 1})
        self.assertEqual(consumer.deserialize(msg_a), {'n': 1})

        # Phase 2: consumers are configured to trust both keys *before*
        # any producer switches.
        consumer = self.get_serializer()
        msg_b = self.get_serializer(key_id='key-b').serialize({'n': 2})
        self.assertEqual(consumer.deserialize(msg_a), {'n': 1})
        self.assertEqual(consumer.deserialize(msg_b), {'n': 2})

        # A stale consumer that only knows key-a rejects the new key
        # explicitly instead of falling back to anything.
        stale = SignedEnvelopeSerializer(keys={'key-a': 'secret-a'})
        self.assertRaises(UnknownKeyIdError, stale.deserialize, msg_b)

        # Phase 3: producers sign with key-b.
        self.assertEqual(consumer.deserialize(msg_b), {'n': 2})

        # Phase 4: key-a is removed; old messages are explicitly rejected.
        consumer = SignedEnvelopeSerializer(keys={'key-b': 'secret-b'})
        self.assertEqual(consumer.deserialize(msg_b), {'n': 2})
        self.assertRaises(UnknownKeyIdError, consumer.deserialize, msg_a)

    def test_same_key_id_different_secret(self):
        producer = SignedEnvelopeSerializer(keys={'key-a': 'secret-a'})
        consumer = SignedEnvelopeSerializer(keys={'key-a': 'wrong-secret'})
        self.assertRaises(BadSignatureError, consumer.deserialize,
                          producer.serialize({'n': 1}))

    def test_tampered_version(self):
        msg = self.get_serializer().serialize({'n': 1})
        self.assertRaises(InvalidEnvelopeError, self.get_serializer().deserialize,
                          msg.replace(b'HUEY:2:', b'HUEY:3:', 1))
        # Even a correctly-signed envelope with an unsupported version is
        # rejected: the version is part of the authenticated header.
        env = self.make_envelope(version=b'3')
        self.assertRaises(InvalidEnvelopeError,
                          self.get_serializer().deserialize, env)

    def test_tampered_key_id(self):
        msg = self.get_serializer().serialize({'n': 1})
        consumer = self.get_serializer()
        # Swapping in another *configured* key id fails verification.
        self.assertRaises(BadSignatureError, consumer.deserialize,
                          msg.replace(b'HUEY:2:key-a:', b'HUEY:2:key-b:', 1))
        # An unconfigured key id is rejected before verification.
        self.assertRaises(UnknownKeyIdError, consumer.deserialize,
                          msg.replace(b'HUEY:2:key-a:', b'HUEY:2:key-z:', 1))
        # Malformed key ids are structural errors.
        env = msg.replace(b'HUEY:2:key-a:', b'HUEY:2:bad!key:', 1)
        self.assertRaises(InvalidEnvelopeError, consumer.deserialize, env)

    def test_tampered_encoding_flag(self):
        msg = self.get_serializer().serialize({'n': 1})
        consumer = self.get_serializer()
        # The flag is authenticated, so flipping it breaks the signature.
        self.assertRaises(BadSignatureError, consumer.deserialize,
                          msg.replace(b'HUEY:2:key-a:n:', b'HUEY:2:key-a:g:', 1))
        # An unknown flag is rejected outright; the consumer never tries
        # other decoders.
        env = self.make_envelope(flag=b'x')
        self.assertRaises(InvalidEnvelopeError, consumer.deserialize, env)

    def test_tampered_payload(self):
        msg = self.get_serializer().serialize({'n': 1})
        signed, signature = msg.rsplit(b':', 1)
        signed = bytearray(signed)
        signed[-1] ^= 0x01
        tampered = bytes(signed) + b':' + signature
        self.assertRaises(BadSignatureError, self.get_serializer().deserialize,
                          tampered)

    def test_tampered_signature(self):
        msg = self.get_serializer().serialize({'n': 1})
        consumer = self.get_serializer()
        self.assertRaises(BadSignatureError, consumer.deserialize,
                          self.tamper_signature(msg))
        self.assertRaises(BadSignatureError, consumer.deserialize, msg[:-2])
        # Missing signature altogether.
        self.assertRaises(InvalidEnvelopeError, consumer.deserialize,
                          b'HUEY:2:key-a:n:abc')

    def test_invalid_envelopes(self):
        consumer = self.get_serializer()
        invalid = [
            b'',
            b'random garbage',
            b'HUEY',
            b'HUEY:2:key-a',
            b'HUEY:2:key-a:n',
            Serializer().serialize({'n': 1}),  # no plain-pickle fallback.
            SignedSerializer(secret='secret-a').serialize({'n': 1}),
        ]
        for data in invalid:
            self.assertRaises(InvalidEnvelopeError, consumer.deserialize, data)

    @unittest.skipIf(gzip is None, 'gzip module not installed')
    def test_invalid_gzip_payload(self):
        env = self.make_envelope(flag=b'g', payload=b'definitely-not-gzip')
        self.assertRaises(InvalidEnvelopeError,
                          self.get_serializer().deserialize, env)

    @unittest.skipIf(zlib is None, 'zlib module not installed')
    def test_invalid_zlib_payload(self):
        env = self.make_envelope(flag=b'z', payload=b'definitely-not-zlib')
        self.assertRaises(InvalidEnvelopeError,
                          self.get_serializer().deserialize, env)

    def install_recorders(self, consumer):
        calls = []
        orig_decompress = consumer._decompress_payload
        orig_deserialize = consumer._deserialize

        def record_decompress(*args):
            calls.append('decompress')
            return orig_decompress(*args)

        def record_deserialize(*args):
            calls.append('deserialize')
            return orig_deserialize(*args)

        consumer._decompress_payload = record_decompress
        consumer._deserialize = record_deserialize
        return calls

    def test_auth_failure_skips_decompress_and_deserialize(self):
        kwargs = {'compression': True} if gzip is not None else {}
        msg = self.get_serializer(**kwargs).serialize({'n': 1})
        consumer = self.get_serializer()
        calls = self.install_recorders(consumer)

        self.assertRaises(BadSignatureError, consumer.deserialize,
                          self.tamper_signature(msg))
        self.assertEqual(calls, [])

        other = SignedEnvelopeSerializer(keys={'key-z': 'secret-z'})
        self.assertRaises(UnknownKeyIdError, consumer.deserialize,
                          other.serialize({'n': 1}))
        self.assertEqual(calls, [])

        self.assertRaises(InvalidEnvelopeError, consumer.deserialize, b'junk')
        self.assertEqual(calls, [])

        # On the success path decompression runs before deserialization.
        self.assertEqual(consumer.deserialize(msg), {'n': 1})
        expected = ['decompress', 'deserialize'] if kwargs else ['deserialize']
        self.assertEqual(calls, expected)

    @unittest.skipIf(gzip is None, 'gzip module not installed')
    def test_gzip_bomb_rejected(self):
        producer = self.get_serializer(compression=True)
        consumer = self.get_serializer(max_decompressed_size=4096)
        calls = self.install_recorders(consumer)
        msg = producer.serialize({'data': b'x' * 1024 * 1024})
        self.assertRaises(PayloadTooLargeError, consumer.deserialize, msg)
        self.assertNotIn('deserialize', calls)

    @unittest.skipIf(zlib is None, 'zlib module not installed')
    def test_zlib_bomb_rejected(self):
        producer = self.get_serializer(compression=True, use_zlib=True)
        consumer = self.get_serializer(max_decompressed_size=4096)
        calls = self.install_recorders(consumer)
        msg = producer.serialize({'data': b'x' * 1024 * 1024})
        self.assertRaises(PayloadTooLargeError, consumer.deserialize, msg)
        self.assertNotIn('deserialize', calls)

    def test_uncompressed_over_limit_rejected(self):
        producer = self.get_serializer()
        consumer = self.get_serializer(max_decompressed_size=64)
        msg = producer.serialize({'data': b'x' * 1024})
        self.assertRaises(PayloadTooLargeError, consumer.deserialize, msg)

    def test_payload_at_limit_accepted(self):
        item = {'data': b'x' * 1000}
        producer = self.get_serializer()
        msg = producer.serialize(item)
        payload = msg.rsplit(b':', 1)[0].split(b':', 4)[4]
        consumer = self.get_serializer(max_decompressed_size=len(payload))
        self.assertEqual(consumer.deserialize(msg), item)
        consumer = self.get_serializer(max_decompressed_size=len(payload) - 1)
        self.assertRaises(PayloadTooLargeError, consumer.deserialize, msg)

    @unittest.skipIf(gzip is None, 'gzip module not installed')
    def test_compressed_payload_at_limit_accepted(self):
        item = {'data': b'x' * 1000}
        producer = self.get_serializer(compression=True)
        msg = producer.serialize(item)
        limit = len(pickle.dumps(item, pickle.HIGHEST_PROTOCOL))
        consumer = self.get_serializer(max_decompressed_size=limit)
        self.assertEqual(consumer.deserialize(msg), item)
        consumer = self.get_serializer(max_decompressed_size=limit - 1)
        self.assertRaises(PayloadTooLargeError, consumer.deserialize, msg)

    def test_legacy_migration_compatibility(self):
        legacy_msg = SignedSerializer(secret='secret-a').serialize({'n': 1})
        # Legacy format is rejected unless migration compatibility is
        # explicitly enabled.
        consumer = self.get_serializer()
        self.assertRaises(InvalidEnvelopeError, consumer.deserialize,
                          legacy_msg)
        consumer = self.get_serializer(allow_legacy=True)
        self.assertEqual(consumer.deserialize(legacy_msg), {'n': 1})
        # A legacy message signed with the wrong key is rejected, not
        # silently accepted or re-interpreted.
        bad_msg = SignedSerializer(secret='secret-b').serialize({'n': 1})
        self.assertRaises(BadSignatureError, consumer.deserialize, bad_msg)

    @unittest.skipIf(gzip is None, 'gzip module not installed')
    def test_legacy_migration_with_compression(self):
        for use_zlib in (False, True):
            if use_zlib and zlib is None:
                continue
            legacy = SignedSerializer(secret='secret-a', compression=True,
                                      use_zlib=use_zlib)
            consumer = self.get_serializer(allow_legacy=True,
                                           compression=True, use_zlib=use_zlib)
            self.assertEqual(consumer.deserialize(legacy.serialize({'n': 1})),
                             {'n': 1})

    def test_legacy_consumers_cannot_read_envelopes(self):
        # Documents the required deployment order: consumers must be
        # upgraded before producers start emitting the new format.
        msg = self.get_serializer().serialize({'n': 1})
        legacy_consumer = SignedSerializer(secret='secret-a')
        self.assertRaises(ValueError, legacy_consumer.deserialize, msg)

    def test_legacy_compat_does_not_weaken_envelope_verification(self):
        consumer = self.get_serializer(allow_legacy=True)
        msg = self.get_serializer().serialize({'n': 1})
        self.assertRaises(BadSignatureError, consumer.deserialize,
                          self.tamper_signature(msg))

    def test_errors_and_logs_do_not_leak_secrets_or_content(self):
        secret = 'super-secret-value'
        content = 'sensitive-task-content'
        producer = SignedEnvelopeSerializer(keys={'key-a': secret})
        consumer = SignedEnvelopeSerializer(keys={'key-a': secret})
        msg = producer.serialize({'task': content})
        other = SignedEnvelopeSerializer(keys={'key-z': secret})

        messages = []
        with self.assertLogs('huey.serializer', level='WARNING') as cm:
            for bad in (self.tamper_signature(msg),
                        other.serialize({'task': content}),
                        b'junk'):
                try:
                    consumer.deserialize(bad)
                except SerializerError as exc:
                    messages.append(str(exc))
                else:
                    self.fail('expected SerializerError')
        self.assertEqual(len(messages), 3)
        haystack = '\n'.join(messages + cm.output)
        self.assertNotIn(secret, haystack)
        self.assertNotIn(content, haystack)
