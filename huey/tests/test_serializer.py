import datetime
import gzip
import logging
import os
import pickle
import tempfile
import threading
import unittest
import zlib
from pathlib import Path

from huey.serializer import SignedSerializer
from huey.serializer import _b64_encode
from huey.serializer import Serializer
from huey.consumer import Worker
from huey.tests.base import BaseTestCase


OLD_SECRET = 'leaked-old-secret-do-not-log'
NEW_SECRET = 'replacement-secret-do-not-log'
ATTACKER_SECRET = 'attacker-secret'
SECRET_MARKER = 'super-secret-task-argument'


def mark_pickle_side_effect(path):
    Path(path).touch()


class PickleSideEffect(object):
    def __init__(self, path):
        self.path = path

    def __reduce__(self):
        return (mark_pickle_side_effect, (self.path,))


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

    def test_serializer_gzip(self):
        self._test_serializer(Serializer(compression=True))

    @unittest.skipIf(not hasattr(zlib, 'compress'), 'zlib not available')
    def test_serializer_zlib(self):
        self._test_serializer(Serializer(compression=True, use_zlib=True))

    @unittest.skipIf(not hasattr(zlib, 'compress'), 'zlib not available')
    def test_mismatched_compression(self):
        for use_zlib in (False, True):
            s = Serializer()
            scomp = Serializer(compression=True, use_zlib=use_zlib)
            for item in self.data:
                self.assertEqual(scomp.deserialize(s.serialize(item)), item)


class TestSignedSerializer(BaseTestCase):
    def setUp(self):
        super(TestSignedSerializer, self).setUp()
        self.tmpdir = tempfile.TemporaryDirectory()
        self.side_effect_path = os.path.join(self.tmpdir.name, 'side-effect')

        @self.huey.task()
        def echo(value):
            return value

        self.echo = echo

    def tearDown(self):
        self.tmpdir.cleanup()
        super(TestSignedSerializer, self).tearDown()

    def strict_serializer(self, **kwargs):
        options = {'secret': NEW_SECRET}
        options.update(kwargs)
        return SignedSerializer(**options)

    def legacy_serializer(self, secret=OLD_SECRET, **kwargs):
        options = {'secret': secret, 'allow_legacy_format': True}
        options.update(kwargs)
        return SignedSerializer(**options)

    def legacy_reader(self, **kwargs):
        options = {
            'secret': NEW_SECRET,
            'legacy_secrets': [OLD_SECRET],
            'allow_legacy_format': True}
        options.update(kwargs)
        return SignedSerializer(**options)

    def legacy_message(self, value, serializer=None, compression=False,
                       use_zlib=False):
        serializer = serializer or self.legacy_serializer()
        signed = serializer._sign(pickle.dumps(value))
        if compression:
            if use_zlib:
                return zlib.compress(signed)
            return gzip.compress(signed)
        return signed

    def task_message(self):
        return self.huey._registry.create_message(self.echo.s(SECRET_MARKER))

    def assert_rejected(self, serializer, raw, marker=SECRET_MARKER,
                        log_expected=True):
        with self.assertLogs('huey.serializer', logging.DEBUG) if \
                log_expected else self._suppress_logs() as logs:
            with self.assertRaises(ValueError) as raised:
                serializer.deserialize(raw)

        self.assertNotIn(NEW_SECRET, str(raised.exception))
        self.assertNotIn(OLD_SECRET, str(raised.exception))
        self.assertNotIn(ATTACKER_SECRET, str(raised.exception))
        self.assertNotIn(marker, str(raised.exception))
        if log_expected:
            output = '\n'.join(logs.output)
            self.assertNotIn(NEW_SECRET, output)
            self.assertNotIn(OLD_SECRET, output)
            self.assertNotIn(ATTACKER_SECRET, output)
            self.assertNotIn(marker, output)

    @staticmethod
    def _suppress_logs():
        import contextlib
        return contextlib.nullcontext()

    def test_single_secret_roundtrip(self):
        serializer = self.strict_serializer()
        self.assertEqual(
            serializer.deserialize(serializer.serialize(SECRET_MARKER)),
            SECRET_MARKER)

    def test_versioned_envelope_and_rotation(self):
        old_writer = SignedSerializer(
            secret_keys={'old': OLD_SECRET, 'new': NEW_SECRET},
            key_id='old')
        rotating_reader = SignedSerializer(
            secret_keys={'old': OLD_SECRET, 'new': NEW_SECRET},
            key_id='new')
        new_writer = self.strict_serializer()

        old_key_message = old_writer.serialize(SECRET_MARKER)
        self.assertTrue(old_key_message.startswith(
            b'huey1:' + _b64_encode(b'old') + b':'))
        self.assertEqual(rotating_reader.deserialize(old_key_message),
                         SECRET_MARKER)

        new_key_message = new_writer.serialize(SECRET_MARKER)
        self.assertTrue(new_key_message.startswith(
            b'huey1:' + _b64_encode(b'default') + b':'))
        self.assertEqual(new_writer.deserialize(new_key_message),
                         SECRET_MARKER)

        old_only = SignedSerializer(secret_keys={'old': OLD_SECRET},
                                    key_id='old')
        self.assert_rejected(old_only, new_key_message, log_expected=True)

    def test_legacy_format_must_be_explicitly_enabled(self):
        legacy_raw = self.legacy_message(SECRET_MARKER)
        strict = self.strict_serializer()

        with self.assertRaises(ValueError):
            strict.deserialize(legacy_raw)

        reader = self.legacy_reader()
        self.assertEqual(reader.deserialize(legacy_raw), SECRET_MARKER)

        reader.allow_legacy_format = False
        with self.assertRaises(ValueError):
            reader.deserialize(legacy_raw)

    def test_legacy_tasks_and_results_gzip_and_zlib(self):
        message = self.task_message()
        eta = datetime.datetime(2030, 1, 1)
        reader = self.legacy_reader()

        for use_zlib in (False, True):
            task_raw = self.legacy_message(
                message, compression=True, use_zlib=use_zlib)
            result_raw = self.legacy_message(
                SECRET_MARKER, compression=True, use_zlib=use_zlib)
            result_key = 'result-%s' % use_zlib

            self.huey.storage.enqueue(task_raw)
            self.huey.storage.add_to_schedule(task_raw, eta)
            self.huey.storage.put_data(result_key, result_raw)

            self.huey.serializer = reader
            task = self.huey.dequeue()
            self.assertEqual(task.data, ((SECRET_MARKER,), {}))
            self.assertEqual(self.huey.execute(task), SECRET_MARKER)
            scheduled, = self.huey.read_schedule(eta)
            self.assertEqual(scheduled.data, ((SECRET_MARKER,), {}))
            self.assertEqual(self.huey.get(result_key), SECRET_MARKER)

    def test_versioned_compressed_tasks_and_results(self):
        writer = self.strict_serializer(compression=True, use_zlib=True)
        reader = self.strict_serializer(compression=False)

        self.huey.serializer = writer
        result = self.echo(SECRET_MARKER)
        self.huey.put('compressed-result', SECRET_MARKER)

        self.huey.serializer = reader
        task = self.huey.dequeue()
        self.assertEqual(task.data, ((SECRET_MARKER,), {}))
        self.assertEqual(self.huey.execute(task), SECRET_MARKER)
        self.assertEqual(result.get(blocking=False), SECRET_MARKER)
        self.assertEqual(self.huey.get('compressed-result'), SECRET_MARKER)

    def test_tampered_envelope_fields_are_rejected(self):
        writer = SignedSerializer(
            secret_keys={'old': OLD_SECRET, 'new': NEW_SECRET},
            key_id='old')
        reader = SignedSerializer(
            secret_keys={'old': OLD_SECRET, 'new': NEW_SECRET},
            key_id='new')
        raw = writer.serialize(SECRET_MARKER)

        tampered = [
            b'huey2' + raw[5:],
            b'huey1::' + b':'.join(raw.split(b':')[2:]),
        ]

        parts = raw.split(b':')
        key_id, flag, payload, signature = parts[1:]
        tampered.extend([
            b':'.join([parts[0], _b64_encode(b'new'), flag, payload,
                       signature]),
            b':'.join([parts[0], key_id,
                       b'0' if flag == b'1' else b'1', payload, signature]),
            b':'.join([parts[0], key_id, flag, payload[:-1] +
                       (b'A' if payload[-1:] != b'A' else b'B'), signature]),
            b':'.join([parts[0], key_id, flag, payload, signature[:-1] +
                       (b'A' if signature[-1:] != b'A' else b'B')]),
        ])

        for candidate in tampered:
            self.assert_rejected(reader, candidate)

    def test_corrupt_envelopes_are_rejected_without_pickle(self):
        strict = self.strict_serializer()
        payload = pickle.dumps(PickleSideEffect(self.side_effect_path))
        for raw in (b'', b'not-an-envelope', b'huey1:bad',
                    b'huey1::' + payload):
            with self.assertRaises(ValueError):
                strict.deserialize(raw)
        self.assertFalse(os.path.exists(self.side_effect_path))

    def test_unknown_key_and_bad_signature_do_not_unpickle(self):
        payload = pickle.dumps(PickleSideEffect(self.side_effect_path))
        strict = self.strict_serializer()

        attacker = SignedSerializer(
            secret_keys={'attacker': ATTACKER_SECRET},
            key_id='attacker')
        unknown_key_raw = attacker._sign_v1(payload, False)
        self.assert_rejected(strict, unknown_key_raw)

        forged = SignedSerializer(
            secret_keys={'default': ATTACKER_SECRET},
            key_id='default')
        bad_signature_raw = forged._sign_v1(payload, False)
        self.assert_rejected(strict, bad_signature_raw)

        attacker_legacy = self.legacy_serializer(secret=ATTACKER_SECRET)
        legacy_raw = attacker_legacy._sign(payload)
        self.assert_rejected(self.legacy_reader(), legacy_raw)

        compressed_attack = gzip.compress(attacker_legacy._sign(payload))
        self.assert_rejected(
            self.legacy_reader(max_decompressed_size=1024 * 1024),
            compressed_attack)

        self.assertFalse(os.path.exists(self.side_effect_path))

        control_path = os.path.join(self.tmpdir.name, 'control')
        trusted = self.strict_serializer()
        trusted.deserialize(
            trusted.serialize(PickleSideEffect(control_path)))
        self.assertTrue(os.path.exists(control_path))

    def test_decompression_is_bounded(self):
        large_value = b'x' * (10 * 1024 * 1024)

        for use_zlib in (False, True):
            writer = self.strict_serializer(
                compression=True, use_zlib=use_zlib,
                max_decompressed_size=64)
            raw = writer.serialize(large_value)
            with self.assertRaises(ValueError):
                writer.deserialize(raw)

        legacy = self.legacy_serializer(max_decompressed_size=64)
        signed = legacy._sign(pickle.dumps(large_value))
        reader = self.legacy_reader(max_decompressed_size=64)
        for use_zlib in (False, True):
            compressed = (zlib.compress if use_zlib else gzip.compress)(signed)
            with self.assertRaises(ValueError):
                reader.deserialize(compressed)

    def test_consumer_logs_neither_secrets_nor_task_content(self):
        payload = pickle.dumps(SECRET_MARKER)
        attacker = self.legacy_serializer(secret=ATTACKER_SECRET)
        self.huey.storage.enqueue(attacker._sign(payload))
        self.huey.serializer = self.legacy_reader()
        worker = Worker(self.huey, threading.Event(), 0, 0, 1)

        with self.assertLogs('huey', logging.WARNING) as logs:
            worker.loop()

        output = '\n'.join(logs.output)
        self.assertNotIn(OLD_SECRET, output)
        self.assertNotIn(NEW_SECRET, output)
        self.assertNotIn(ATTACKER_SECRET, output)
        self.assertNotIn(SECRET_MARKER, output)
