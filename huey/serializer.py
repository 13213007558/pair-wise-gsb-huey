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
import json
import logging
import pickle
import sys

from huey.exceptions import ConfigurationError
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


class JSONSerializer(Serializer):
    message_envelope = '__huey_message__'

    def _serialize(self, data):
        from huey.registry import Message
        payload = self._encode_message(data, Message)
        return json.dumps(payload, default=self._default).encode('utf-8')

    def _deserialize(self, data):
        payload = json.loads(data.decode('utf-8'))
        from huey.registry import Message

        return self._decode_message(payload, Message)

    def _encode_message(self, value, message_class):
        if isinstance(value, message_class):
            return {
                self.message_envelope: True,
                'fields': list(value._fields),
                'values': [self._encode_message(item, message_class)
                           for item in value],
                'schema': value.schema,
                'original_schema': getattr(
                    value, 'original_schema', None),
                'original_args': self._encode_message(
                    getattr(value, 'original_args', None), message_class),
                'original_kwargs': self._encode_message(
                    getattr(value, 'original_kwargs', None), message_class),
            }
        if isinstance(value, (list, tuple)):
            return [self._encode_message(item, message_class)
                    for item in value]
        if isinstance(value, dict):
            return dict((key, self._encode_message(item, message_class))
                        for key, item in value.items())
        return value

    def _decode_message(self, value, message_class):
        if (isinstance(value, dict) and
                value.get(self.message_envelope) is True):
            fields = value.get('fields')
            values = value.get('values')
            if fields != list(message_class._fields):
                raise ValueError('Invalid Huey JSON message fields')
            if not isinstance(values, list):
                raise ValueError('Invalid Huey JSON message values')
            if len(values) != len(message_class._fields):
                raise ValueError('Invalid Huey JSON message field count')
            values = [self._decode_message(item, message_class)
                      for item in values]
            message = message_class(*values, schema=value.get('schema'))
            for name in ('original_schema', 'original_args',
                         'original_kwargs'):
                if value.get(name) is not None:
                    setattr(message, name, value[name])
            return message
        if isinstance(value, list):
            return [self._decode_message(item, message_class)
                    for item in value]
        if isinstance(value, dict):
            return dict((key, self._decode_message(item, message_class))
                        for key, item in value.items())
        return value

    def _default(self, value):
        raise TypeError('Object of type %s is not JSON serializable' %
                        type(value).__name__)


class SignedJSONSerializer(SignedSerializer, JSONSerializer):
    def _serialize(self, message):
        data = JSONSerializer._serialize(self, message)
        return self._sign(data)

    def _deserialize(self, data):
        return JSONSerializer._deserialize(self, self._unsign(data))
