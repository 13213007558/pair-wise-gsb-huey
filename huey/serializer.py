import gzip
import hashlib
import hmac
import logging
import pickle
import struct
import zlib

from huey.exceptions import ConfigurationError
logger = logging.getLogger('huey.serializer')


ENVELOPE_MAGIC = b'HUEY'
ENVELOPE_VERSION = 1
ENVELOPE_SIGNATURE_SIZE = hashlib.sha256().digest_size
ENVELOPE_FLAG_COMPRESSED = 0x01
ENVELOPE_FLAG_ZLIB = 0x02
ENVELOPE_VALID_FLAGS = (0, ENVELOPE_FLAG_COMPRESSED,
                        ENVELOPE_FLAG_COMPRESSED | ENVELOPE_FLAG_ZLIB)
DEFAULT_MAX_DECOMPRESSED_SIZE = 64 * 1024 * 1024

INVALID_ENVELOPE = 'Invalid signed message.'
INVALID_KEY = 'Invalid signing key.'
INVALID_COMPRESSED_MESSAGE = 'Message could not be decompressed.'


def is_compressed(data):
    return data and (data[0] == 0x1f or data[0] == 0x78)


def _is_gzip(data):
    return data[:2] == b'\x1f\x8b'


def _is_zlib(data):
    return (
        len(data) >= 2 and
        data[0] == 0x78 and
        struct.unpack('>H', data[:2])[0] % 31 == 0)


def bounded_decompress(data, use_zlib=False,
                       max_size=DEFAULT_MAX_DECOMPRESSED_SIZE):
    if not isinstance(data, bytes) or not data:
        raise ValueError(INVALID_COMPRESSED_MESSAGE)

    wbits = 15 if use_zlib else 31
    result = bytearray()
    remaining = data

    try:
        while remaining:
            decompressor = zlib.decompressobj(wbits)

            while not decompressor.eof:
                capacity = max_size - len(result) + 1
                before = len(remaining)
                chunk = decompressor.decompress(remaining, capacity)
                remaining = decompressor.unconsumed_tail
                result.extend(chunk)

                if len(result) > max_size:
                    raise ValueError(INVALID_COMPRESSED_MESSAGE)
                if len(remaining) == before and not chunk:
                    raise ValueError(INVALID_COMPRESSED_MESSAGE)

            capacity = max_size - len(result) + 1
            chunk = decompressor.flush(capacity)
            result.extend(chunk)
            if len(result) > max_size:
                raise ValueError(INVALID_COMPRESSED_MESSAGE)

            remaining = decompressor.unused_data
            if use_zlib and remaining:
                raise ValueError(INVALID_COMPRESSED_MESSAGE)
    except zlib.error:
        raise ValueError(INVALID_COMPRESSED_MESSAGE)

    return bytes(result)


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
    def __init__(self, secret=None, salt='huey', secrets=None,
                 active_key_id='default', accept_legacy=False,
                 legacy_secret=None,
                 max_decompressed_size=DEFAULT_MAX_DECOMPRESSED_SIZE,
                 **kwargs):
        super(SignedSerializer, self).__init__(**kwargs)
        if (secret is None) == (secrets is None):
            raise ConfigurationError('The secret and salt parameters are '
                                     'required by %r' % type(self))
        if not salt:
            raise ConfigurationError('The secret and salt parameters are '
                                     'required by %r' % type(self))

        if not isinstance(max_decompressed_size, int) or max_decompressed_size <= 0:
            raise ConfigurationError('max_decompressed_size must be a '
                                     'positive integer.')

        self.salt = self._encode_key_part(salt, 'salt')
        self.max_decompressed_size = max_decompressed_size
        self.accept_legacy = accept_legacy

        if secret is not None:
            secret_value = self._encode_key_value(secret)
            self.secret = secret_value
            self.secrets = {b'default': secret_value}
            self.active_key_id = b'default'
        else:
            if not isinstance(secrets, dict) or not secrets:
                raise ConfigurationError('secrets must be a non-empty '
                                         'mapping.')
            self.secrets = {
                self._encode_key_part(key, 'key id'):
                    self._encode_key_value(value)
                for key, value in secrets.items()
            }
            self.active_key_id = self._encode_key_part(active_key_id,
                                                       'active key id')
            if self.active_key_id not in self.secrets:
                raise ConfigurationError('active_key_id must identify a '
                                         'configured secret.')
            self.secret = self.secrets[self.active_key_id]

        if legacy_secret is not None:
            self.legacy_secret = self._encode_key_value(legacy_secret)
        elif accept_legacy and secret is not None:
            self.legacy_secret = self.secret
        else:
            self.legacy_secret = None

        if accept_legacy and self.legacy_secret is None:
            raise ConfigurationError('legacy_secret is required when '
                                     'accept_legacy is enabled with secrets.')

        self.separator = b':'
        self._key = hashlib.sha1(self.salt + self.secret).digest()

    def _encode_key_part(self, value, description):
        if isinstance(value, bytes):
            encoded = value
        elif isinstance(value, str):
            encoded = value.encode('utf8')
        else:
            raise ConfigurationError('%s must be a string or bytes.' %
                                     description)
        if not encoded:
            raise ConfigurationError('%s cannot be empty.' % description)
        return encoded

    def _encode_key_value(self, value):
        if isinstance(value, bytes):
            encoded = value
        elif isinstance(value, str):
            encoded = value.encode('utf8')
        else:
            raise ConfigurationError('secrets must be strings or bytes.')
        if not encoded:
            raise ConfigurationError('secrets cannot be empty.')
        return encoded

    def _envelope_key(self, key_id):
        return hmac.new(
            self.secrets[key_id],
            msg=b'huey-signed-v1\x00' + self.salt + b'\x00' + key_id,
            digestmod=hashlib.sha256).digest()

    def _envelope_signature(self, authenticated, key_id):
        return hmac.new(self._envelope_key(key_id), msg=authenticated,
                        digestmod=hashlib.sha256).digest()

    def _signature(self, message):
        signature = hmac.new(self._key, msg=message, digestmod=hashlib.sha1)
        return signature.hexdigest().encode('utf8')

    def _legacy_signature(self, message, secret):
        key = hashlib.sha1(self.salt + secret).digest()
        signature = hmac.new(key, msg=message, digestmod=hashlib.sha1)
        return signature.hexdigest().encode('utf8')

    def _sign(self, message):
        return message + self.separator + self._signature(message)

    def _unsign(self, signed):
        return self._unsign_legacy(signed, self.secret)

    def _unsign_legacy(self, signed, secret):
        try:
            msg, sig = signed.rsplit(self.separator, 1)
        except ValueError:
            raise ValueError(INVALID_ENVELOPE)

        expected = self._legacy_signature(msg, secret)
        if constant_time_compare(sig, expected):
            return msg

        raise ValueError(INVALID_ENVELOPE)

    def _pack_envelope(self, payload):
        flags = 0
        if self.comp:
            flags |= ENVELOPE_FLAG_COMPRESSED
            if self.use_zlib:
                flags |= ENVELOPE_FLAG_ZLIB

        key_id = self.active_key_id
        header = struct.pack('>4sBBH', ENVELOPE_MAGIC, ENVELOPE_VERSION,
                             flags, len(key_id))
        prefix = header + key_id + struct.pack('>Q', len(payload))
        signature = self._envelope_signature(prefix + payload, key_id)
        return prefix + payload + signature

    def _unpack_envelope(self, signed):
        header_size = 8
        length_size = 8
        min_size = (header_size + 1 + length_size +
                    ENVELOPE_SIGNATURE_SIZE)
        if not isinstance(signed, bytes) or len(signed) < min_size:
            raise ValueError(INVALID_ENVELOPE)

        magic, version, flags, key_id_size = struct.unpack(
            '>4sBBH', signed[:header_size])
        if magic != ENVELOPE_MAGIC or version != ENVELOPE_VERSION:
            raise ValueError(INVALID_ENVELOPE)
        if flags not in ENVELOPE_VALID_FLAGS:
            raise ValueError(INVALID_ENVELOPE)

        key_id_start = header_size
        payload_length_start = key_id_start + key_id_size
        payload_start = payload_length_start + length_size
        payload_end = payload_start

        if len(signed) < payload_start + ENVELOPE_SIGNATURE_SIZE:
            raise ValueError(INVALID_ENVELOPE)

        key_id = signed[key_id_start:payload_length_start]
        payload_length, = struct.unpack('>Q', signed[payload_length_start:
                                                     payload_start])
        payload_end = payload_start + payload_length
        signature_end = payload_end + ENVELOPE_SIGNATURE_SIZE
        if signature_end != len(signed):
            raise ValueError(INVALID_ENVELOPE)

        secret = self.secrets.get(key_id)
        if secret is None:
            raise ValueError(INVALID_KEY)

        payload = signed[payload_start:payload_end]
        signature = signed[payload_end:signature_end]
        authenticated = signed[:payload_start] + payload
        expected = self._envelope_signature(authenticated, key_id)
        if not constant_time_compare(signature, expected):
            raise ValueError(INVALID_ENVELOPE)

        if flags & ENVELOPE_FLAG_COMPRESSED:
            payload = bounded_decompress(
                payload,
                use_zlib=bool(flags & ENVELOPE_FLAG_ZLIB),
                max_size=self.max_decompressed_size)

        return payload

    def _read_legacy(self, data):
        try:
            return self._unsign_legacy(data, self.legacy_secret)
        except ValueError:
            if not isinstance(data, bytes) or not (
                    _is_gzip(data) or _is_zlib(data)):
                raise

        payload = bounded_decompress(
            data,
            use_zlib=_is_zlib(data),
            max_size=self.max_decompressed_size)
        return self._unsign_legacy(payload, self.legacy_secret)

    def serialize(self, data):
        payload = self._serialize(data)
        if self.comp:
            if self.use_zlib:
                payload = zlib.compress(payload, self.comp_level)
            else:
                payload = gzip.compress(payload, self.comp_level)
        return self._pack_envelope(payload)

    def deserialize(self, data):
        if not isinstance(data, bytes):
            raise ValueError(INVALID_ENVELOPE)

        is_envelope = (
            len(data) >= 5 and
            data[:4] == ENVELOPE_MAGIC and
            data[4] == ENVELOPE_VERSION)

        if is_envelope:
            payload = self._unpack_envelope(data)
        elif self.accept_legacy:
            payload = self._read_legacy(data)
        else:
            raise ValueError(INVALID_ENVELOPE)

        try:
            return self._deserialize(payload)
        except Exception:
            raise ValueError(INVALID_ENVELOPE)

    def _serialize(self, message):
        return Serializer._serialize(self, message)

    def _deserialize(self, data):
        return Serializer._deserialize(self, data)
