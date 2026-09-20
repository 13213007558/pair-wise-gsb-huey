try:
    import gzip
except ImportError:
    gzip = None
try:
    import zlib
except ImportError:
    zlib = None
import hashlib
import hmac
import logging
import pickle
import sys

import struct
from io import BytesIO

from huey.exceptions import (
    ConfigurationError,
    InvalidEnvelopeError,
    PayloadTooLargeError,
    SignatureMismatchError,
    UnknownKeyError)
from huey.utils import encode


logger = logging.getLogger('huey.serializer')


if gzip is not None:
    if sys.version_info[0] > 2:
        gzip_compress = gzip.compress
        gzip_decompress = gzip.decompress
    else:
        from io import BytesIO

        def gzip_compress(data, comp_level):
            buf = BytesIO()
            fh = gzip.GzipFile(fileobj=buf, mode='wb',
                               compresslevel=comp_level)
            fh.write(data)
            fh.close()
            return buf.getvalue()

        def gzip_decompress(data):
            buf = BytesIO(data)
            fh = gzip.GzipFile(fileobj=buf, mode='rb')
            try:
                return fh.read()
            finally:
                fh.close()


if sys.version_info[0] == 2:
    def is_compressed(data):
        return data and (data[0] == b'\x1f' or data[0] == b'\x78')
else:
    def is_compressed(data):
        return data and data[0] == 0x1f or data[0] == 0x78


class Serializer(object):
    def __init__(self, compression=False, compression_level=6, use_zlib=False,
                 pickle_protocol=pickle.HIGHEST_PROTOCOL):
        self.comp = compression
        self.comp_level = compression_level
        self.use_zlib = use_zlib
        self.pickle_protocol = pickle_protocol or pickle.HIGHEST_PROTOCOL
        if self.comp:
            if self.use_zlib and zlib is None:
                raise ConfigurationError('use_zlib specified, but zlib module '
                                         'not found.')
            elif gzip is None:
                raise ConfigurationError('gzip module required to enable '
                                         'compression.')

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
                data = gzip_compress(data, self.comp_level)
        return data

    def deserialize(self, data):
        if self.comp:
            if not is_compressed(data):
                logger.warning('compression enabled but message data does not '
                               'appear to be compressed.')
            elif self.use_zlib:
                data = zlib.decompress(data)
            else:
                data = gzip_decompress(data)
        return self._deserialize(data)


def constant_time_compare(s1, s2):
    return hmac.compare_digest(s1, s2)


class SignedSerializer(Serializer):
    def __init__(self, secret=None, salt='huey', **kwargs):
        super(SignedSerializer, self).__init__(**kwargs)
        if not secret or not salt:
            raise ConfigurationError('The secret and salt parameters are '
                                     'required by %r' % type(self))
        self.secret = encode(secret)
        self.salt = encode(salt)
        self.separator = b':'
        self._key = hashlib.sha1(self.salt + self.secret).digest()

    def _signature(self, message):
        signature = hmac.new(self._key, msg=message, digestmod=hashlib.sha1)
        return signature.hexdigest().encode('utf8')

    def _sign(self, message):
        return message + self.separator + self._signature(message)

    def _unsign(self, signed):
        if self.separator not in signed:
            raise ValueError('Separator "%s" not found' % self.separator)

        msg, sig = signed.rsplit(self.separator, 1)
        if constant_time_compare(sig, self._signature(msg)):
            return msg

        raise ValueError('Signature "%s" mismatch!' % sig)

    def _serialize(self, message):
        data = super(SignedSerializer, self)._serialize(message)
        return self._sign(data)

    def _deserialize(self, data):
        return super(SignedSerializer, self)._deserialize(self._unsign(data))


class SignedEnvelopeSerializer(Serializer):
    magic = b'HUEY'
    envelope_version = 1
    sig_length = 32  # Raw HMAC-SHA256 digest.

    ENCODING_NONE = 0
    ENCODING_GZIP = 1
    ENCODING_ZLIB = 2
    encoding_names = {ENCODING_NONE: 'none', ENCODING_GZIP: 'gzip',
                      ENCODING_ZLIB: 'zlib'}

    def __init__(self, keys=None, current_key_id=None, salt='huey',
                 max_decompressed_size=None, allow_legacy=False,
                 legacy_secret=None, legacy_salt='huey', **kwargs):
        """
        Versioned, signed message envelope supporting key rotation.

        Envelope layout (big-endian): magic b'HUEY' (4 bytes), version
        (1 byte), length-prefixed utf-8 key id, encoding flag (1 byte:
        0=none, 1=gzip, 2=zlib), payload, and a 32-byte HMAC-SHA256
        signature over everything preceding it. The signature covers
        the version, key id, encoding flag and the on-the-wire (still
        compressed) payload, so decompression and pickle deserialization
        only happen after the signature has been verified.

        ``keys`` maps key ids to secrets. Producers sign with
        ``current_key_id``; consumers accept any key in ``keys``. To
        rotate, add the new key to consumers, switch producers to the
        new key id, then remove the old key -- messages signed with a
        removed key are rejected with UnknownKeyError.

        Errors are distinguishable and never fall back to plain pickle
        or to trying every encoding: InvalidEnvelopeError (malformed or
        unsupported envelope), UnknownKeyError, SignatureMismatchError
        and PayloadTooLargeError (decompressed payload exceeds
        ``max_decompressed_size``).

        The old SignedSerializer format is only accepted when
        ``allow_legacy=True`` (requires ``legacy_secret``). The legacy
        format authenticates *after* decompression, so the legacy branch
        necessarily decompresses unauthenticated data -- enable it only
        while migrating. The compatibility branch is never used for
        messages carrying the new envelope magic and cannot bypass
        verification of new envelopes.
        """
        super(SignedEnvelopeSerializer, self).__init__(**kwargs)
        if not keys or not isinstance(keys, dict):
            raise ConfigurationError('%r requires a non-empty dict of '
                                     'key-id -> secret.' % type(self))
        if not salt:
            raise ConfigurationError('The salt parameter is required by %r'
                                     % type(self))
        self.salt = encode(salt)
        self._keys = {}
        for key_id, secret in keys.items():
            if not key_id or not secret:
                raise ConfigurationError('key ids and secrets must be '
                                         'non-empty.')
            key_id = encode(key_id)
            if len(key_id) > 255:
                raise ConfigurationError('key ids must be <= 255 bytes.')
            self._keys[key_id] = self._derive_key(secret)
        if current_key_id is not None:
            current_key_id = encode(current_key_id)
            if current_key_id not in self._keys:
                raise ConfigurationError('current_key_id not present in '
                                         'keys.')
        self.current_key_id = current_key_id
        self.max_decompressed_size = max_decompressed_size
        self.allow_legacy = allow_legacy
        if allow_legacy:
            if not legacy_secret:
                raise ConfigurationError('allow_legacy requires a '
                                         'legacy_secret.')
            self._legacy = SignedSerializer(
                secret=legacy_secret, salt=legacy_salt,
                compression=self.comp, compression_level=self.comp_level,
                use_zlib=self.use_zlib)
        else:
            self._legacy = None

    def _derive_key(self, secret):
        return hashlib.sha256(self.salt + encode(secret)).digest()

    def _sign(self, key, message):
        return hmac.new(key, msg=message, digestmod=hashlib.sha256).digest()

    def _pack_envelope(self, key_id, encoding, payload):
        header = struct.pack('!4sBB', self.magic, self.envelope_version,
                             len(key_id)) + key_id
        header += struct.pack('!B', encoding)
        signed = header + payload
        return signed + self._sign(self._keys[key_id], signed)

    def _unpack_envelope(self, data):
        fixed = 6 + 1 + self.sig_length  # magic+ver+kidlen, encoding, sig.
        if len(data) < fixed + 1:
            raise InvalidEnvelopeError('message too short to be a valid '
                                       'envelope.')
        magic, version, key_id_len = struct.unpack('!4sBB', data[:6])
        if magic != self.magic:
            raise InvalidEnvelopeError('bad envelope magic.')
        if key_id_len == 0:
            raise InvalidEnvelopeError('empty key id.')
        if len(data) < fixed + key_id_len:
            raise InvalidEnvelopeError('truncated envelope.')
        key_id = data[6:6 + key_id_len]
        encoding = struct.unpack('!B', data[6 + key_id_len:7 + key_id_len])[0]
        signed = data[:-self.sig_length]
        payload = data[7 + key_id_len:-self.sig_length]
        signature = data[-self.sig_length:]
        return version, key_id, encoding, payload, signed, signature

    def serialize(self, data):
        if self.current_key_id is None:
            raise ConfigurationError('current_key_id is required to '
                                     'serialize messages.')
        payload = self._serialize(data)
        encoding = self.ENCODING_NONE
        if self.comp:
            if self.use_zlib:
                payload = zlib.compress(payload, self.comp_level)
                encoding = self.ENCODING_ZLIB
            else:
                payload = gzip_compress(payload, self.comp_level)
                encoding = self.ENCODING_GZIP
        return self._pack_envelope(self.current_key_id, encoding, payload)

    def deserialize(self, data):
        if not data.startswith(self.magic):
            if self.allow_legacy:
                logger.warning('accepting legacy signed message; the legacy '
                               'format authenticates after decompression.')
                return self._deserialize_legacy(data)
            raise InvalidEnvelopeError('unrecognized message format.')

        version, key_id, encoding, payload, signed, signature = \
            self._unpack_envelope(data)
        key = self._keys.get(key_id)
        if key is None:
            logger.warning('rejecting message with unknown key id %r.',
                           key_id.decode('utf8', 'replace'))
            raise UnknownKeyError('message signed with unknown key id: %r'
                                  % key_id.decode('utf8', 'replace'))
        if not constant_time_compare(signature, self._sign(key, signed)):
            logger.warning('signature mismatch for key id %r.',
                           key_id.decode('utf8', 'replace'))
            raise SignatureMismatchError('message signature mismatch.')
        if version != self.envelope_version:
            raise InvalidEnvelopeError('unsupported envelope version: %d'
                                       % version)
        if encoding not in self.encoding_names:
            raise InvalidEnvelopeError('unsupported encoding flag: %d'
                                       % encoding)

        # Authentication succeeded -- only now may we decompress and
        # deserialize the (previously untrusted) payload.
        if encoding != self.ENCODING_NONE:
            payload = self._decompress(encoding, payload)
        self._check_size(payload)
        return self._deserialize(payload)

    def _deserialize_legacy(self, data):
        return self._legacy.deserialize(data)

    def _check_size(self, payload):
        limit = self.max_decompressed_size
        if limit is not None and len(payload) > limit:
            logger.warning('payload of %d bytes exceeds limit of %d bytes.',
                           len(payload), limit)
            raise PayloadTooLargeError(
                'payload of %d bytes exceeds limit of %d bytes.'
                % (len(payload), limit))

    def _decompress(self, encoding, data):
        if encoding == self.ENCODING_GZIP:
            if gzip is None:
                raise InvalidEnvelopeError('gzip module not available.')
            return self._gunzip(data)
        elif encoding == self.ENCODING_ZLIB:
            if zlib is None:
                raise InvalidEnvelopeError('zlib module not available.')
            return self._unzlib(data)
        raise InvalidEnvelopeError('unsupported encoding flag: %d' % encoding)

    def _gunzip(self, data):
        limit = self.max_decompressed_size
        fh = gzip.GzipFile(fileobj=BytesIO(data), mode='rb')
        try:
            # Read at most limit+1 bytes so a compression bomb is stopped
            # before its full contents are realized.
            if limit is None:
                out = fh.read()
            else:
                out = fh.read(limit + 1)
        except (IOError, EOFError) as exc:
            raise InvalidEnvelopeError('invalid gzip payload: %s' % exc)
        finally:
            fh.close()
        self._check_size(out)
        return out

    def _unzlib(self, data):
        limit = self.max_decompressed_size
        decomp = zlib.decompressobj()
        try:
            if limit is None:
                out = decomp.decompress(data)
            else:
                out = decomp.decompress(data, limit + 1)
                if decomp.unconsumed_tail:
                    raise PayloadTooLargeError(
                        'decompressed payload exceeds limit of %d bytes.'
                        % limit)
            out += decomp.flush()
        except zlib.error as exc:
            raise InvalidEnvelopeError('invalid zlib payload: %s' % exc)
        self._check_size(out)
        return out
