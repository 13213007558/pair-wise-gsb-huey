import calendar
import datetime
import os
import struct
import time
from collections import namedtuple
import errno
import sys
import warnings
try:
    import fcntl
except ImportError:
    fcntl = None
try:
    from zoneinfo import ZoneInfo
except ImportError:
    try:
        from backports.zoneinfo import ZoneInfo
    except ImportError:
        ZoneInfo = None

if sys.version_info < (3, 12):
    utcnow = datetime.datetime.utcnow
else:
    def utcnow():
        return (datetime.datetime
                .now(datetime.timezone.utc)
                .replace(tzinfo=None))


Error = namedtuple('Error', ('metadata',))


class UTC(datetime.tzinfo):
    zero = datetime.timedelta(0)

    def __repr__(self):
        return "<UTC>"
    def utcoffset(self, dt):
        return self.zero
    def tzname(self, dt):
        return "UTC"
    def dst(self, dt):
        return self.zero
_UTC = UTC()


class LocalTimezone(datetime.tzinfo):
    def utcoffset(self, dt):
        if dt is None:
            return None
        return datetime.timedelta(seconds=-time.timezone if self._is_standard(dt)
                                  else -time.altzone)

    def dst(self, dt):
        if dt is None or self._is_standard(dt):
            return datetime.timedelta(0)
        return datetime.timedelta(seconds=time.timezone - time.altzone)

    def tzname(self, dt):
        return time.tzname[0 if self._is_standard(dt) else 1]

    def _is_standard(self, dt):
        timestamp = time.mktime(dt.replace(tzinfo=None).timetuple())
        return time.localtime(timestamp).tm_isdst == 0


_LOCAL = LocalTimezone()
utc = _UTC


def get_timezone(value):
    if value is None or value is False:
        return _LOCAL
    if value is True:
        return _UTC
    if isinstance(value, datetime.tzinfo):
        return value
    if ZoneInfo is None:
        raise ValueError('ZoneInfo is required to resolve timezone ' +
                         text_type(value))
    return ZoneInfo(value)


def load_class(s):
    path, klass = s.rsplit('.', 1)
    __import__(path)
    mod = sys.modules[path]
    return getattr(mod, klass)


def reraise_as(new_exc_class):
    exc_class, exc, tb = sys.exc_info()
    raise new_exc_class('%s: %s' % (exc_class.__name__, exc))


def is_naive(dt):
    """
    Determines if a given datetime.datetime is naive.
    The concept is defined in Python's docs:
    http://docs.python.org/library/datetime.html#datetime.tzinfo
    Assuming value.tzinfo is either None or a proper datetime.tzinfo,
    value.utcoffset() implements the appropriate logic.
    """
    return dt.utcoffset() is None


def make_naive(dt):
    """
    Makes an aware datetime.datetime naive in local time zone.
    """
    tt = dt.utctimetuple()
    ts = calendar.timegm(tt)
    local_tt = time.localtime(ts)
    return datetime.datetime(*local_tt[:6])


def naive_utc(dt):
    if is_naive(dt):
        return dt
    return aware_to_utc(dt)


def aware_utc(dt):
    if is_naive(dt):
        return dt.replace(tzinfo=_UTC)
    return dt.astimezone(_UTC)


def naive_local(dt):
    if not is_naive(dt):
        return make_naive(dt)
    return dt


def local_timestamp(dt):
    if is_naive(dt):
        return dt.replace(tzinfo=_LOCAL)
    return dt.astimezone(_LOCAL)


def local_to_utc_timestamp(dt):
    if is_naive(dt):
        return dt.replace(tzinfo=_LOCAL)
    return dt.astimezone(_UTC)


def normalize_schedule_time(value, timezone=None):
    if value is None:
        return None
    tzinfo = get_timezone(timezone)
    if is_naive(value):
        value = value.replace(tzinfo=tzinfo)
    return value.astimezone(_UTC).replace(tzinfo=None)


def pack_periodic_state(value):
    return b'hp2\0' + struct.pack('>Q', int((calendar.timegm(
        value.timetuple()) + value.microsecond * 1e-6) * 1000000))


def unpack_periodic_state(value):
    if value is None:
        return None
    from huey.constants import EmptyData
    if value is EmptyData:
        return None
    if not isinstance(value, bytes):
        if isinstance(value, memoryview):
            value = value.tobytes()
        else:
            value = value.encode('utf8')
    if value.startswith(b'hp2\0') and len(value) == 12:
        microseconds = struct.unpack('>Q', value[4:])[0]
        return datetime.datetime.utcfromtimestamp(microseconds / 1000000.0)
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number > 100000000000:
        number /= 1000000.0
    return datetime.datetime.utcfromtimestamp(number)


def aware_to_utc(dt):
    """
    Converts an aware datetime.datetime in UTC time zone.
    """
    return dt.astimezone(_UTC).replace(tzinfo=None)


def local_to_utc(dt):
    """
    Converts a naive local datetime.datetime in UTC time zone.
    """
    return datetime.datetime(*time.gmtime(time.mktime(dt.timetuple()))[:6])


def normalize_expire_time(expires, utc=True, timezone=None):
    if isinstance(expires, datetime.datetime):
        return normalize_time(eta=expires, utc=utc, timezone=timezone)
    return normalize_time(delay=expires, utc=utc)


def normalize_time(eta=None, delay=None, utc=True, timezone=None):
    if not ((delay is None) ^ (eta is None)):
        raise ValueError('Specify either an eta (datetime) or delay (seconds)')
    elif delay:
        method = (utc and utcnow or
                  datetime.datetime.now)
        if not isinstance(delay, datetime.timedelta):
            delay = datetime.timedelta(seconds=delay)
        return method() + delay
    elif eta:
        has_tz = not is_naive(eta)
        if utc:
            if not has_tz:
                if timezone is not None:
                    eta = normalize_schedule_time(eta, timezone)
                else:
                    eta = local_to_utc(eta)
            else:
                eta = aware_to_utc(eta)
        elif has_tz:
            # Convert TZ-aware into naive localtime.
            eta = make_naive(eta)
        return eta


if sys.version_info[0] == 2:
    string_type = basestring
    text_type = unicode
    def to_timestamp(dt):
        if is_naive(dt):
            return calendar.timegm(dt.timetuple()) + (dt.microsecond * 1e-6)
        return dt.timestamp()
else:
    string_type = (bytes, str)
    text_type = str
    def to_timestamp(dt):
        return to_timestamp_utc(dt, True)


def to_timestamp_utc(dt, utc=True):
    if is_naive(dt):
        if utc:
            return calendar.timegm(dt.timetuple()) + (dt.microsecond * 1e-6)
        return local_to_utc_timestamp(dt).timestamp()
    return dt.timestamp()


def encode(s):
    if isinstance(s, bytes):
        return s
    elif isinstance(s, text_type):
        return s.encode('utf8')
    elif s is not None:
        return text_type(s).encode('utf8')


def decode(s):
    if isinstance(s, text_type):
        return s
    elif isinstance(s, bytes):
        return s.decode('utf8')
    elif s is not None:
        return text_type(s)


class FileLock(object):
    def __init__(self, filename):
        if fcntl is None:
            warnings.warn('FileLock not supported on this platform. Please '
                          'use a different storage implementation.')
        self.filename = filename
        self.fd = None

        dirname = os.path.dirname(filename)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        elif os.path.exists(self.filename):
            os.unlink(self.filename)

    def acquire(self):
        flags = os.O_CREAT | os.O_TRUNC | os.O_RDWR
        self.fd = os.open(self.filename, flags)
        if fcntl is not None:
            fcntl.flock(self.fd, fcntl.LOCK_EX)

    def release(self):
        if self.fd is not None:
            fd, self.fd = self.fd, None
            if fcntl is not None:
                fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


if sys.version_info[0] < 3:
    time_clock = time.time
else:
    time_clock = time.monotonic
