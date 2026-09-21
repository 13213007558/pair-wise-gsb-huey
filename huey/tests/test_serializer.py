try:
    import gzip
except ImportError:
    gzip = None
import hashlib
import hmac
import os
import pickle
import struct
import tempfile
import threading
import traceback
import unittest
from unittest.mock import patch
try:
    import zlib
except ImportError:
    zlib = None

from huey.api import MemoryHuey
from huey.consumer import Worker
from huey import serializer as serializer_module
from huey.serializer import ENVELOPE_MAGIC
from huey.serializer import ENVELOPE_VERSION
from huey.serializer import SignedSerializer
from huey.serializer import Serializer
from huey.tests.base import BaseTestCase


MALICIOUS_PICKLE_PATH = os.path.join(tempfile.gettempdir(),
                                     'huey-test-malicious-pickle')


class MaliciousPayload(object):
    def __reduce__(self):
        return (os.unlink, (MALICIOUS_PICKLE_PATH,))


def _legacy_sign(payload, secret, salt=b'huey', compression=False,
                 use_zlib=False):
    if isinstance(secret, str):
        secret = secret.encode('utf8')
    key = hashlib.sha1(salt + secret).digest()
    signature = hmac.new(key, payload, hashlib.sha1).hexdigest().encode()
    signed = payload + b':' + signature
    if compression:
        if use_zlib:
            return zlib.compress(signed)
        return gzip.compress(signed)
    return signed


def _legacy_message(message, **kwargs):
    return _legacy_sign(pickle.dumps(message, pickle.HIGHEST_PROTOCOL),
                        **kwargs)


def _create_marker():
    with open(MALICIOUS_PICKLE_PATH, 'w'):
        pass


class TestSerializer(BaseTestCase):
    data = [
        None,
        0, 1,
        b'a' * 1024,
        ['k1', 'k2', 'k3'],
        {'k1': 'v1', 'k2': 'v2', 'k3': 'v3'}]

    def tearDown(self):
        try:
            os.remove(MALICIOUS_PICKLE_PATH)
        except OSError:
            pass
        super().tearDown()

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

    def test_signed_envelope_roundtrip(self):
        s = SignedSerializer(secret='single secret')
        self._test_serializer(s)
        serialized = s.serialize(self.data[-1])
        self.assertEqual(serialized[:4], ENVELOPE_MAGIC)
        self.assertEqual(serialized[4], ENVELOPE_VERSION)
        self.assertEqual(serialized[7:8], struct.pack('>H', 7)[1:2])
        self.assertEqual(serialized[8:15], b'default')

    @unittest.skipIf(zlib is None, 'zlib module not installed')
    @unittest.skipIf(gzip is None, 'gzip module not installed')
    def test_signed_envelope_compression(self):
        self._test_serializer(SignedSerializer(secret='s', compression=True))
        self._test_serializer(SignedSerializer(secret='s', compression=True,
                                               use_zlib=True))

    def test_key_rotation_reads_old_and_new_and_writes_active_key(self):
        old = SignedSerializer(secrets={'old': 'old-secret',
                                        'new': 'new-secret'},
                               active_key_id='old')
        rotated = SignedSerializer(secrets={'old': 'old-secret',
                                            'new': 'new-secret'},
                                   active_key_id='new')
        old_message = old.serialize('queued message')
        self.assertEqual(old_message[8:11], b'old')
        self.assertEqual(rotated.deserialize(old_message), 'queued message')

        new_message = rotated.serialize('new message')
        self.assertEqual(new_message[8:11], b'new')
        self.assertEqual(rotated.deserialize(new_message), 'new message')

        strict = SignedSerializer(secret='new-secret')
        with self.assertRaises(ValueError):
            strict.deserialize(old_message)

    @unittest.skipIf(zlib is None, 'zlib module not installed')
    @unittest.skipIf(gzip is None, 'gzip module not installed')
    def test_legacy_uncompressed_and_compressed(self):
        for use_zlib in (False, True):
            legacy = _legacy_message(
                'legacy-%s' % use_zlib,
                secret='old-secret',
                compression=use_zlib,
                use_zlib=use_zlib)
            reader = SignedSerializer(secret='new-secret',
                                      accept_legacy=True,
                                      legacy_secret='old-secret',
                                      compression=True,
                                      use_zlib=use_zlib)
            self.assertEqual(reader.deserialize(legacy),
                             'legacy-%s' % use_zlib)

    def test_legacy_compatibility_is_explicit(self):
        legacy = _legacy_message('legacy', secret='old-secret')
        reader = SignedSerializer(secret='new-secret',
                                  legacy_secret='old-secret')
        with self.assertRaises(ValueError):
            reader.deserialize(legacy)

        reader.accept_legacy = True
        self.assertEqual(reader.deserialize(legacy), 'legacy')

    @unittest.skipIf(zlib is None, 'zlib module not installed')
    def test_decompression_is_size_limited(self):
        payload = b'\0' * (256 * 1024)

        new = SignedSerializer(secret='new-secret', compression=True,
                               use_zlib=True,
                               max_decompressed_size=1024)
        with self.assertRaises(ValueError):
            new.deserialize(new.serialize(payload))

        legacy_bomb = zlib.compress(payload)
        legacy_reader = SignedSerializer(secret='new-secret',
                                         accept_legacy=True,
                                         legacy_secret='old-secret',
                                         max_decompressed_size=1024)
        with self.assertRaises(ValueError):
            legacy_reader.deserialize(legacy_bomb)

    @unittest.skipIf(zlib is None, 'zlib module not installed')
    @unittest.skipIf(gzip is None, 'gzip module not installed')
    def test_rotated_tasks_and_results_with_compression(self):
        reader = SignedSerializer(secrets={'new': 'new-secret'},
                                  active_key_id='new',
                                  accept_legacy=True,
                                  legacy_secret='old-secret',
                                  compression=True)
        huey = MemoryHuey('rotation-test', serializer=reader, utc=False)

        @huey.task()
        def echo(value):
            return value

        old_task = echo.s('old-task')
        huey.storage.enqueue(_legacy_message(
            huey._registry.create_message(old_task),
            secret='old-secret',
            compression=True))
        self.assertEqual(huey.dequeue(), old_task)
        self.assertEqual(huey.execute(old_task), 'old-task')

        new_result = echo('new-task')
        self.assertEqual(huey.execute(huey.dequeue()), 'new-task')
        self.assertEqual(new_result.get(), 'new-task')

        huey.storage.put_data('old-result', _legacy_message(
            'old-result',
            secret='old-secret',
            compression=True,
            use_zlib=True))
        self.assertEqual(huey.get('old-result'), 'old-result')

        huey.put_result('new-result', 'new-result')
        self.assertEqual(huey.get('new-result'), 'new-result')

    def test_invalid_envelopes_rejected_before_pickle(self):
        secret = 'known-secret'
        serializer = SignedSerializer(secret=secret)
        valid = serializer.serialize({'safe': 'value'})
        corrupt = {
            'empty': b'',
            'non-bytes': None,
            'truncated': valid[:-20],
            'extra-byte': valid + b'x',
            'version': valid[:4] + bytes([2]) + valid[5:],
            'compression-flag': valid[:5] + bytes([1]) + valid[6:],
            'key-id': valid[:8] + b'x' + valid[9:],
            'payload': valid[:-40] + bytes([valid[-40] ^ 1]) + valid[-39:],
            'signature': valid[:-1] + bytes([valid[-1] ^ 1]),
        }

        attacker = SignedSerializer(secret='attacker-secret')
        _create_marker()
        corrupt['unknown-key'] = attacker.serialize(MaliciousPayload())
        bad_known_signature = serializer.serialize(MaliciousPayload())
        corrupt['bad-known-signature'] = (
            bad_known_signature[:-1] +
            bytes([bad_known_signature[-1] ^ 1]))

        legacy_bad_sig = _legacy_message(MaliciousPayload(), secret='other')
        legacy_reader = SignedSerializer(secret=secret, accept_legacy=True,
                                         legacy_secret=secret)

        _create_marker()
        for name, data in corrupt.items():
            with self.subTest(name), \
                    patch.object(serializer_module.pickle, 'loads') as loads:
                with self.assertRaises(ValueError):
                    serializer.deserialize(data)
                loads.assert_not_called()
        self.assertTrue(os.path.exists(MALICIOUS_PICKLE_PATH))

        with patch.object(serializer_module.pickle, 'loads') as loads:
            with self.assertRaises(ValueError):
                legacy_reader.deserialize(legacy_bad_sig)
            loads.assert_not_called()
        self.assertTrue(os.path.exists(MALICIOUS_PICKLE_PATH))

        if zlib is not None and gzip is not None:
            for use_zlib in (False, True):
                legacy_compressed_bad_sig = _legacy_message(
                    MaliciousPayload(), secret='other', compression=True,
                    use_zlib=use_zlib)
                with patch.object(serializer_module.pickle,
                                  'loads') as loads:
                    with self.assertRaises(ValueError):
                        legacy_reader.deserialize(
                            legacy_compressed_bad_sig)
                    loads.assert_not_called()
                self.assertTrue(os.path.exists(MALICIOUS_PICKLE_PATH))

    def test_malicious_pickle_control_and_error_redaction(self):
        _create_marker()
        trusted = SignedSerializer(secret='trusted-secret')
        trusted.deserialize(trusted.serialize(MaliciousPayload()))
        self.assertFalse(os.path.exists(MALICIOUS_PICKLE_PATH))

        _create_marker()
        secret = 'super-secret-value'
        confidential = 'CONFIDENTIAL-TASK-CONTENT'
        serializer = SignedSerializer(secret=secret)
        attacker = SignedSerializer(secret='attacker-secret')
        invalid = attacker.serialize(confidential)

        try:
            serializer.deserialize(invalid)
        except ValueError as exc:
            rendered = ''.join(traceback.format_exception(
                type(exc), exc, exc.__traceback__))
            self.assertNotIn(secret, rendered)
            self.assertNotIn(confidential, rendered)
        else:
            self.fail('unknown key was accepted')

        huey = MemoryHuey('redaction-test', serializer=serializer, utc=False)
        huey.storage.enqueue(invalid)
        worker = Worker(huey, threading.Event(), 0.001, 0.001, 1)
        with self.assertLogs('huey.consumer.Worker') as logs:
            worker.loop()
        rendered = '\n'.join(logs.output)
        self.assertNotIn(secret, rendered)
        self.assertNotIn(confidential, rendered)
        self.assertIn('Error reading from queue', rendered)
        self.assertTrue(os.path.exists(MALICIOUS_PICKLE_PATH))
