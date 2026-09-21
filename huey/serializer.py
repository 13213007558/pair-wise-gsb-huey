import base64
import binascii
import gzip
import hashlib
import hmac
import logging
import pickle
import zlib

from huey.exceptions import ConfigurationError
from huey.utils import encode


logger = logging.getLogger('huey.serializer')


ENVELOPE_VERSION = 1
ENVELOPE_PREFIX = b'huey1'
ENVELOPE_SEPARATOR = b':'
DEFAULT_KEY_ID = 'default'
DEFAULT_MAX_DECOMPRESSED_SIZE = 16 * 1024 * 1024


def is_compressed(data):
    return data and (data[0] == 0x1f or data[0] == 0x78)


def _b64_decode(data):
    try:
        padded = data + (b'=' * (-len(data) % 4))
        return base64.b64decode(padded, altchars=b'-_', validate=True)
    except (binascii.Error, ValueError, TypeError):
        raise ValueError('Invalid envelope encoding.')


def _b64_encode(data):
    return base64.urlsafe_b64encode(data).rstrip(b'=')


def _bounded_stream(data, max_size, wbits):
    decompressor = zlib.decompressobj(wbits)
    chunks = []
    total = 0
    remaining = data
    trailing = b''
    while remaining and not decompressor.eof:
        part = remaining[:64]
        chunk = decompressor.decompress(part)
        total += len(chunk)
        if total > max_size:
            raise ValueError('Message exceeds decompressed size limit.')
        chunks.append(chunk)
        if decompressor.eof:
            trailing = decompressor.unused_data
            break
        if decompressor.unconsumed_tail:
            remaining = decompressor.unconsumed_tail
        else:
            remaining = remaining[len(part):]

    chunk = decompressor.flush()
    total += len(chunk)
    if total > max_size:
        raise ValueError('Message exceeds decompressed size limit.')
    chunks.append(chunk)
    return b''.join(chunks), trailing


def _bounded_gzip(data, max_size):
    try:
        return _bounded_stream(data, max_size, zlib.MAX_WBITS | 16)
    except (OSError, EOFError, zlib.error):
        raise ValueError('Invalid compressed message.')


def _bounded_zlib(data, max_size):
    try:
        return _bounded_stream(data, max_size, zlib.MAX_WBITS)
    except (OSError, EOFError, zlib.error):
        raise ValueError('Invalid compressed message.')


def bounded_decompress(data, max_size, allow_trailing=False):
    if not data or max_size <= 0:
        raise ValueError('Invalid compressed message.')
    if data[:2] == b'\x1f\x8b':
        payload, trailing = _bounded_gzip(data, max_size)
    elif data[:2] in (b'\x78\x01', b'\x78\x5e', b'\x78\x9c', b'\x78\xda'):
        payload, trailing = _bounded_zlib(data, max_size)
    else:
        raise ValueError('Invalid compressed message.')
    if trailing and not allow_trailing:
        raise ValueError('Invalid compressed message.')
    return payload, trailing


class Serializer(object):
    def __init__(self, compression=False, compression_level=6, use_zlib=False,
                 pickle_protocol=pickle.HIGHEST_PROTOCOL):
        self.comp = compression
        self.comp_level = compression_level
        self.use_zlib = use_zlib
        self.pickle_protocol = pickle_protocol or pickle.HIGHEST_PROTOCOL

    def _serialize(self, data):
        return pickle.dumps(data, self.pickle_protocol)

    def _deserialize(self, data):
        return pickle.loads(data)

    def serialize(self, data):
        data = self._serialize(data)
        if self.comp:
            if self.use_zlib:
                data = zlib.compress(data, self.comp_level)
            else:
                data = gzip.compress(data, self.comp_level)
        return data

    def deserialize(self, data):
        if self.comp:
            if not is_compressed(data):
                logger.warning('compression enabled but message data does not '
                               'appear to be compressed.')
            elif self.use_zlib:
                data = zlib.decompress(data)
            else:
                data = gzip.decompress(data)
        return self._deserialize(data)


def constant_time_compare(s1, s2):
    return hmac.compare_digest(s1, s2)


class SignedSerializer(Serializer):
    def __init__(self, secret=None, salt='huey', key_id=None,
                 secret_keys=None, legacy_secrets=None,
                 allow_legacy_format=False,
                 max_decompressed_size=DEFAULT_MAX_DECOMPRESSED_SIZE,
                 **kwargs):
        super(SignedSerializer, self).__init__(**kwargs)
        if not salt:
            raise ConfigurationError('The secret and salt parameters are '
                                     'required by %r' % type(self))
        self.salt = encode(salt)
        self.separator = ENVELOPE_SEPARATOR

        if secret_keys is not None:
            if secret is not None:
                raise ConfigurationError('Specify either secret or '
                                         'secret_keys, not both.')
        else:
            if not secret:
                raise ConfigurationError('The secret and salt parameters are '
                                         'required by %r' % type(self))
            key_id = key_id or DEFAULT_KEY_ID
            secret_keys = {key_id: secret}

        if not secret_keys:
            raise ConfigurationError('A signing key is required.')
        if key_id is not None and key_id not in secret_keys:
            raise ConfigurationError('Unknown current key id.')

        self._keys = {}
        for configured_id, configured_secret in secret_keys.items():
            if not configured_id or not configured_secret:
                raise ConfigurationError('Key ids and secrets are required.')
            self._keys[encode(configured_id)] = encode(configured_secret)

        self.key_id = encode(key_id or next(iter(secret_keys)))
        if self.key_id not in self._keys:
            raise ConfigurationError('Unknown current key id.')
        self.secret = self._keys[self.key_id]

        if isinstance(legacy_secrets, (str, bytes)):
            legacy_secrets = [legacy_secrets]
        elif legacy_secrets is None:
            legacy_secrets = []
        legacy_secrets = list(legacy_secrets)
        if legacy_secrets and not allow_legacy_format:
            raise ConfigurationError('Legacy secrets require '
                                     'allow_legacy_format=True.')
        if secret is not None and allow_legacy_format:
            legacy_secrets.insert(0, secret)

        self.allow_legacy_format = bool(allow_legacy_format)
        self._legacy_keys = [self._legacy_key(encode(value))
                             for value in legacy_secrets if value]
        if not isinstance(max_decompressed_size, int) or \
                max_decompressed_size <= 0:
            raise ConfigurationError('max_decompressed_size must be a '
                                     'positive integer.')
        self.max_decompressed_size = max_decompressed_size

    def _derived_key(self, secret, label):
        return hmac.new(self.salt, label + secret,
                        digestmod=hashlib.sha256).digest()

    def _key(self, key_id):
        return self._derived_key(self._keys[key_id], b'huey-message-v1')

    def _legacy_key(self, secret):
        return hashlib.sha1(self.salt + secret).digest()

    def _signature(self, message, key):
        return hmac.new(key, msg=message,
                        digestmod=hashlib.sha256).digest()

    def _legacy_signature(self, message, key):
        return hmac.new(key, msg=message,
                        digestmod=hashlib.sha1).hexdigest().encode('utf8')

    def _authenticated_parts(self, key_id, compressed, payload):
        return (
            ENVELOPE_PREFIX,
            _b64_encode(key_id),
            b'1' if compressed else b'0',
            _b64_encode(payload))

    def _sign_v1(self, message, compressed):
        parts = self._authenticated_parts(self.key_id, compressed, message)
        authenticated = self.separator.join(parts)
        signature = _b64_encode(self._signature(authenticated,
                                                self._key(self.key_id)))
        return authenticated + self.separator + signature

    def _unsign_v1(self, signed):
        parts = signed.split(self.separator)
        if len(parts) != 5 or parts[0] != ENVELOPE_PREFIX:
            raise ValueError('Invalid signed message envelope.')

        encoded_key_id, flag, encoded_payload, encoded_signature = parts[1:]
        if flag not in (b'0', b'1'):
            raise ValueError('Invalid signed message envelope.')

        key_id = _b64_decode(encoded_key_id)
        if not key_id:
            raise ValueError('Invalid signed message envelope.')
        if key_id not in self._keys:
            raise ValueError('Message signed with unknown key id.')

        authenticated = self.separator.join(parts[:4])
        signature = _b64_decode(encoded_signature)
        expected = self._signature(authenticated, self._key(key_id))
        if not constant_time_compare(signature, expected):
            raise ValueError('Invalid message signature.')

        payload = _b64_decode(encoded_payload)
        if flag == b'1':
            payload, _ = bounded_decompress(payload,
                                            self.max_decompressed_size)
        return payload

    def _sign(self, message):
        if not self.allow_legacy_format or not self._legacy_keys:
            raise ValueError('Legacy message writing is not supported.')
        return message + self.separator + self._legacy_signature(
            message, self._legacy_keys[0])

    def _unsign(self, signed):
        if not self.allow_legacy_format:
            raise ValueError('Legacy message format is disabled.')
        if signed.startswith(ENVELOPE_PREFIX + self.separator):
            raise ValueError('Invalid legacy signed message.')
        if self.separator not in signed:
            raise ValueError('Invalid legacy signed message.')

        message, signature = signed.rsplit(self.separator, 1)
        for key in self._legacy_keys:
            if constant_time_compare(signature,
                                     self._legacy_signature(message, key)):
                return message

        logger.debug('Rejected legacy message with invalid signature.')
        raise ValueError('Invalid legacy message signature.')

    def _unsign_legacy(self, signed):
        if signed[:2] == b'\x1f\x8b' or signed[:2] in (
                b'\x78\x01', b'\x78\x5e', b'\x78\x9c', b'\x78\xda'):
            payload, trailing = bounded_decompress(
                signed, self.max_decompressed_size, allow_trailing=True)
            signed = payload + trailing
        return self._unsign(signed)

    def serialize(self, data):
        message = self._serialize(data)
        if self.comp:
            if self.use_zlib:
                message = zlib.compress(message, self.comp_level)
            else:
                message = gzip.compress(message, self.comp_level)
        return self._sign_v1(message, self.comp)

    def deserialize(self, data):
        try:
            if isinstance(data, memoryview):
                data = data.tobytes()
            if not isinstance(data, bytes):
                raise ValueError('Invalid signed message envelope.')

            if data.startswith(ENVELOPE_PREFIX + self.separator):
                payload = self._unsign_v1(data)
            else:
                payload = self._unsign_legacy(data)
            return self._deserialize(payload)
        except ValueError:
            logger.warning('Rejected invalid signed message.')
            raise
        except ConfigurationError:
            raise
        except (OSError, EOFError, zlib.error):
            logger.warning('Rejected malformed signed message.')
            raise ValueError('Invalid signed message.')
        except Exception:
            logger.warning('Rejected unreadable signed message.')
            raise ValueError('Invalid signed message.')

    def _serialize(self, message):
        return super(SignedSerializer, self)._serialize(message)

    def _deserialize(self, data):
        return super(SignedSerializer, self)._deserialize(data)
