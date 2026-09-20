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
import re
import sys

from huey.exceptions import ConfigurationError
from huey.exceptions import BadSignatureError
from huey.exceptions import InvalidEnvelopeError
from huey.exceptions import PayloadTooLargeError
from huey.exceptions import UnknownKeyIdError
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


def _gzip_decompress_limited(data, limit):
    if gzip is None:
        raise InvalidEnvelopeError('gzip payload but gzip module is not '
                                   'available.')
    if limit is None:
        try:
            return gzip_decompress(data)
        except (IOError, EOFError) as exc:
            raise InvalidEnvelopeError('invalid gzip payload: %s' %
                                       type(exc).__name__)
    from io import BytesIO
    try:
        fh = gzip.GzipFile(fileobj=BytesIO(data), mode='rb')
        try:
            out = fh.read(limit + 1)
        finally:
            fh.close()
    except (IOError, EOFError) as exc:
        raise InvalidEnvelopeError('invalid gzip payload: %s' %
                                   type(exc).__name__)
    if len(out) > limit:
        raise PayloadTooLargeError('decompressed payload exceeds limit of %d '
                                   'bytes' % limit)
    return out


def _zlib_decompress_limited(data, limit):
    if zlib is None:
        raise InvalidEnvelopeError('zlib payload but zlib module is not '
                                   'available.')
    if limit is None:
        try:
            return zlib.decompress(data)
        except zlib.error:
            raise InvalidEnvelopeError('invalid zlib payload')
    dobj = zlib.decompressobj()
    try:
        out = dobj.decompress(data, limit + 1)
    except zlib.error:
        raise InvalidEnvelopeError('invalid zlib payload')
    if len(out) > limit or dobj.unconsumed_tail:
        raise PayloadTooLargeError('decompressed payload exceeds limit of %d '
                                   'bytes' % limit)
    try:
        out += dobj.flush()
    except zlib.error:
        raise InvalidEnvelopeError('invalid zlib payload')
    if len(out) > limit:
        raise PayloadTooLargeError('decompressed payload exceeds limit of %d '
                                   'bytes' % limit)
    if not dobj.eof:
        raise InvalidEnvelopeError('truncated zlib payload')
    return out


class SignedEnvelopeSerializer(Serializer):
    """Versioned, authenticated message envelope with key rotation.

    Wire format::

        HUEY:<version>:<key-id>:<encoding-flag>:<payload>:<hex-signature>

    The HMAC-SHA256 signature covers the version, key id, encoding flag and
    the on-the-wire payload. Decompression and deserialization only happen
    after the signature has been verified.

    Unlike SignedSerializer (the legacy format), the signed blob is not
    itself compressed, so consumers never have to decompress a message
    before they can authenticate it. The legacy format authenticates
    *after* decompression, which is its inherent limitation.

    :param dict keys: mapping of key id -> secret. Every configured key id
        may be used to verify messages; removing a key id causes messages
        signed with it to be rejected with UnknownKeyIdError.
    :param str key_id: the key id used to sign new messages. Defaults to
        the only configured key id when a single key is present.
    :param str secret: convenience alternative to ``keys``; requires
        ``key_id`` and configures a single key.
    :param str salt: salt used when deriving per-key HMAC keys.
    :param bool compression: compress new messages (the encoding is
        recorded per-message in the envelope).
    :param int compression_level: 0 for least, 9 for most.
    :param bool use_zlib: use zlib instead of gzip for new messages.
    :param int max_decompressed_size: maximum allowed size (in bytes) of
        a payload after decompression. Larger payloads are rejected with
        PayloadTooLargeError. ``None`` disables the limit.
    :param bool allow_legacy: also accept the legacy SignedSerializer
        format. Disabled by default. Note that the legacy format
        authenticates *after* decompression, so enabling this re-exposes
        the consumer to unauthenticated decompression of legacy messages.
    :param str legacy_key_id: which configured key verifies legacy
        messages (defaults to the signing key id).
    """
    magic = b'HUEY'
    version = b'2'
    separator = b':'
    flag_none = b'n'
    flag_gzip = b'g'
    flag_zlib = b'z'
    _flags = (flag_none, flag_gzip, flag_zlib)
    _key_id_re = re.compile(br'^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$')

    def __init__(self, keys=None, key_id=None, secret=None, salt='huey',
                 compression=False, compression_level=6, use_zlib=False,
                 max_decompressed_size=None, allow_legacy=False,
                 legacy_key_id=None, **kwargs):
        super(SignedEnvelopeSerializer, self).__init__(
            compression=compression, compression_level=compression_level,
            use_zlib=use_zlib, **kwargs)
        if secret is not None:
            if keys is not None:
                raise ConfigurationError('specify either "keys" or '
                                         '"secret", not both.')
            if not key_id:
                raise ConfigurationError('"key_id" is required when '
                                         '"secret" is used.')
            keys = {key_id: secret}
        if not keys:
            raise ConfigurationError('%r requires at least one key.' %
                                     type(self).__name__)
        if not salt:
            raise ConfigurationError('The salt parameter is required by %r' %
                                     type(self).__name__)
        self.salt = encode(salt)
        self._secrets = {}
        self._keys = {}
        for kid, ksecret in keys.items():
            kid = encode(kid)
            if not self._key_id_re.match(kid):
                raise ConfigurationError('invalid key id %r: must be 1-64 '
                                         'chars of [A-Za-z0-9._-]' % kid)
            self._secrets[kid] = encode(ksecret)
            self._keys[kid] = hashlib.sha256(
                self.salt + self._secrets[kid]).digest()
        if key_id is None:
            if len(self._keys) != 1:
                raise ConfigurationError('"key_id" is required when '
                                         'multiple keys are configured.')
            key_id = list(self._keys)[0]
        key_id = encode(key_id)
        if key_id not in self._keys:
            raise ConfigurationError('signing key id not present in '
                                     '"keys".')
        self.key_id = key_id
        if max_decompressed_size is not None and max_decompressed_size < 0:
            raise ConfigurationError('max_decompressed_size must be >= 0.')
        self.max_decompressed_size = max_decompressed_size
        self.allow_legacy = allow_legacy
        self.legacy_key_id = (encode(legacy_key_id) if legacy_key_id
                              else self.key_id)
        if allow_legacy and self.legacy_key_id not in self._keys:
            raise ConfigurationError('legacy key id not present in "keys".')

    def _signature(self, message, key_id):
        signature = hmac.new(self._keys[key_id], msg=message,
                             digestmod=hashlib.sha256)
        return signature.hexdigest().encode('ascii')

    def serialize(self, data):
        payload = self._serialize(data)
        if self.comp:
            if self.use_zlib:
                payload = zlib.compress(payload, self.comp_level)
                flag = self.flag_zlib
            else:
                payload = gzip_compress(payload, self.comp_level)
                flag = self.flag_gzip
        else:
            flag = self.flag_none
        header = self.separator.join((self.magic, self.version, self.key_id,
                                      flag))
        signed = header + self.separator + payload
        return signed + self.separator + self._signature(signed, self.key_id)

    def deserialize(self, data):
        if not data.startswith(self.magic + self.separator):
            return self._deserialize_legacy(data)
        signed, sep, signature = data.rpartition(self.separator)
        if not sep:
            raise InvalidEnvelopeError('envelope is missing a signature.')
        parts = signed.split(self.separator, 4)
        if len(parts) != 5:
            raise InvalidEnvelopeError('malformed envelope header.')
        magic, version, key_id, flag, payload = parts
        if version != self.version:
            raise InvalidEnvelopeError('unsupported envelope version.')
        if flag not in self._flags:
            raise InvalidEnvelopeError('unknown encoding flag.')
        if not self._key_id_re.match(key_id):
            raise InvalidEnvelopeError('malformed key id.')
        if key_id not in self._keys:
            logger.warning('rejecting message: unknown key id.')
            raise UnknownKeyIdError('message signed with an unknown key id.')
        if not constant_time_compare(signature,
                                     self._signature(signed, key_id)):
            logger.warning('rejecting message: signature mismatch.')
            raise BadSignatureError('message signature mismatch.')
        return self._deserialize(self._decompress_payload(flag, payload))

    def _decompress_payload(self, flag, payload):
        limit = self.max_decompressed_size
        if flag == self.flag_none:
            if limit is not None and len(payload) > limit:
                raise PayloadTooLargeError(
                    'payload exceeds limit of %d bytes' % limit)
            return payload
        elif flag == self.flag_gzip:
            return _gzip_decompress_limited(payload, limit)
        return _zlib_decompress_limited(payload, limit)

    def _deserialize_legacy(self, data):
        if not self.allow_legacy:
            logger.warning('rejecting legacy message: migration '
                           'compatibility is not enabled.')
            raise InvalidEnvelopeError(
                'message is not a versioned envelope and legacy format '
                'support is not enabled.')
        # The legacy format compressed the *signed* blob, so decompression
        # unavoidably happens before authentication here. The decompressed
        # size limit is still enforced.
        legacy = SignedSerializer(secret=self._secrets[self.legacy_key_id],
                                  salt=self.salt,
                                  pickle_protocol=self.pickle_protocol)
        if self.comp:
            flag = self.flag_zlib if self.use_zlib else self.flag_gzip
            data = self._decompress_payload(flag, data)
        try:
            return legacy.deserialize(data)
        except ValueError:
            logger.warning('rejecting legacy message: signature mismatch.')
            raise BadSignatureError('legacy message signature mismatch.')
