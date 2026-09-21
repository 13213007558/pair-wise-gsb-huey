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
import datetime

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


class JsonSerializer(Serializer):
    type_marker = '__huey_type__'
    value_marker = '__huey_value__'

    def _serialize(self, data):
        return json.dumps(self._encode_value(data),
                          separators=(',', ':')).encode('utf8')

    def _deserialize(self, data):
        if isinstance(data, bytes):
            data = data.decode('utf8')
        return json.loads(data, object_hook=self._decode_hook)

    def _tag(self, type_name, value):
        return {self.type_marker: type_name, self.value_marker: value}

    def _parse_datetime(self, value):
        try:
            return datetime.datetime.fromisoformat(value)
        except AttributeError:
            fmt = '%Y-%m-%dT%H:%M:%S.%f' if '.' in value else \
                    '%Y-%m-%dT%H:%M:%S'
            return datetime.datetime.strptime(value, fmt)

    def _parse_time(self, value):
        try:
            return datetime.time.fromisoformat(value)
        except AttributeError:
            fmt = '%H:%M:%S.%f' if '.' in value else '%H:%M:%S'
            return datetime.datetime.strptime(value, fmt).time()

    def _encode_value(self, obj):
        if isinstance(obj, dict):
            return dict((key, self._encode_value(value))
                        for key, value in obj.items())
        if isinstance(obj, list):
            return [self._encode_value(value) for value in obj]
        if hasattr(obj, '_fields'):
            from huey.registry import Message
            from huey.registry import VersionedMessage
            if isinstance(obj, Message):
                return self._tag('Message', [self._encode_value(value)
                                             for value in obj])
            if isinstance(obj, VersionedMessage):
                return self._tag('VersionedMessage',
                                 [self._encode_value(value) for value in obj])
        if isinstance(obj, tuple):
            return self._tag('tuple', [self._encode_value(value)
                                       for value in obj])
        if isinstance(obj, set):
            return self._tag('set', [self._encode_value(value)
                                     for value in obj])
        if isinstance(obj, bytes):
            return self._tag('bytes', obj.decode('latin1'))
        if isinstance(obj, datetime.datetime):
            return self._tag('datetime', obj.isoformat())
        if isinstance(obj, datetime.date):
            return self._tag('date', obj.isoformat())
        if isinstance(obj, datetime.time):
            return self._tag('time', obj.isoformat())
        if isinstance(obj, datetime.timedelta):
            return self._tag('timedelta', obj.total_seconds())
        return obj

    def _decode_hook(self, obj):
        if set(obj) == {self.type_marker, self.value_marker}:
            type_name = obj[self.type_marker]
            value = obj[self.value_marker]
            if type_name == 'Message':
                from huey.registry import Message
                return Message(*value)
            if type_name == 'VersionedMessage':
                from huey.registry import VersionedMessage
                return VersionedMessage(*value)
            if type_name == 'tuple':
                return tuple(value)
            if type_name == 'set':
                return set(value)
            if type_name == 'bytes':
                return value.encode('latin1')
            if type_name == 'datetime':
                return self._parse_datetime(value)
            if type_name == 'date':
                return datetime.datetime.strptime(value, '%Y-%m-%d').date()
            if type_name == 'time':
                return self._parse_time(value)
            if type_name == 'timedelta':
                return datetime.timedelta(seconds=value)
            raise ValueError('Unsupported Huey JSON type %r' % type_name)
        return obj


class SignedJsonSerializer(SignedSerializer, JsonSerializer):
    def _serialize(self, message):
        data = JsonSerializer._serialize(self, message)
        return self._sign(data)

    def _deserialize(self, data):
        return JsonSerializer._deserialize(self, self._unsign(data))
