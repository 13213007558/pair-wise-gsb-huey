"""
Schedule definitions for periodic tasks.

Schedules compute their occurrences on a UTC baseline and only map those
instants into a local timezone (via zoneinfo) when matching cron-style
wall-clock fields. This keeps the sequence of occurrences comparable and
deterministic regardless of the timezone of the host machine, so workers
deployed in different timezones (e.g. UTC and Asia/Shanghai) produce
identical results.

DST policy for cron-style schedules:

* When a local wall-clock time is skipped (spring forward), no occurrence
  is generated for that day.
* When a local wall-clock time occurs twice (fall back), the occurrence
  fires only once, at the first (fold=0) instant.

Interval schedules are anchored in UTC and are therefore unaffected by
DST transitions altogether.
"""
import datetime
import re

from huey.utils import get_timezone
from huey.utils import is_naive
from huey.utils import to_utc


UTC = datetime.timezone.utc
EPOCH = datetime.datetime(1970, 1, 1, tzinfo=UTC)
ONE_MINUTE = datetime.timedelta(minutes=1)
ZERO = datetime.timedelta(0)


def minute_floor(dt):
    return dt.replace(second=0, microsecond=0)


class Schedule(object):
    """
    Base class for periodic task schedules.

    :param tz: timezone used to interpret naive start/end datetimes and,
        for cron-style schedules, the timezone whose wall-clock is matched
        against the cron fields. May be a tzinfo instance or an IANA name
        such as "Asia/Shanghai". When not specified, naive datetimes are
        interpreted as UTC and cron fields are matched against the Huey
        instance's default timezone (UTC when huey was created with
        utc=True, otherwise the system local timezone).
    :param start: optional naive or aware datetime before which no
        occurrence is generated. Naive values are interpreted in tz
        (default UTC).
    :param end: optional naive or aware datetime after which no occurrence
        is generated. Naive values are interpreted in tz (default UTC).
    """
    def __init__(self, tz=None, start=None, end=None):
        self.tz = get_timezone(tz)
        self.start = self._normalize_bound(start)
        self.end = self._normalize_bound(end)

    def _normalize_bound(self, dt):
        # Naive bounds are interpreted in the schedule's timezone, which
        # defaults to UTC. The result is always an aware UTC datetime.
        if dt is None:
            return None
        return to_utc(dt, self.tz or UTC)

    def _in_bounds(self, ts):
        if self.start is not None and ts < self.start:
            return False
        if self.end is not None and ts > self.end:
            return False
        return True

    def validate_datetime(self, timestamp):
        """
        Legacy interface: return True when the given (typically naive)
        timestamp matches the schedule.
        """
        raise NotImplementedError

    def __call__(self, timestamp):
        return self.validate_datetime(timestamp)

    def initial_occurrence(self, now, default_tz=None):
        """
        Return the occurrence instant for a schedule that has no recorded
        last-run state, or None. The default policy only fires when "now"
        itself is an occurrence, mirroring Huey's historical behavior of
        not backfilling occurrences missed before deployment.
        """
        raise NotImplementedError

    def due_occurrence(self, last_run, now, default_tz=None):
        """
        Return the latest occurrence in the half-open interval
        (last_run, now], or None when no occurrence is due. Both arguments
        and the return value are aware UTC datetimes.
        """
        raise NotImplementedError


class IntervalSchedule(Schedule):
    """
    Run a periodic task every N seconds.

    Occurrences are computed using UTC arithmetic anchored at the start
    time (or the Unix epoch when no start is given), so the sequence is
    completely independent of the host timezone and DST transitions.
    """
    def __init__(self, every, tz=None, start=None, end=None):
        if isinstance(every, (int, float)):
            every = datetime.timedelta(seconds=every)
        if not isinstance(every, datetime.timedelta) or every <= ZERO:
            raise ValueError('every must be a positive number of seconds '
                             'or a datetime.timedelta')
        super(IntervalSchedule, self).__init__(tz=tz, start=start, end=end)
        self.every = every
        self.anchor = self.start if self.start is not None else EPOCH

    def __repr__(self):
        return '<IntervalSchedule every=%s>' % self.every

    def validate_datetime(self, timestamp):
        ts = to_utc(timestamp, self.tz or UTC)
        if not self._in_bounds(ts):
            return False
        delta = ts - self.anchor
        return delta >= ZERO and delta % self.every == ZERO

    def initial_occurrence(self, now, default_tz=None):
        if not self._in_bounds(now):
            return None
        delta = now - self.anchor
        if delta >= ZERO and delta % self.every == ZERO:
            return now
        return None

    def due_occurrence(self, last_run, now, default_tz=None):
        hi = now
        if self.end is not None and hi > self.end:
            hi = self.end
        delta = hi - self.anchor
        if delta < ZERO:
            return None
        occurrence = self.anchor + (delta // self.every) * self.every
        if last_run is not None and occurrence <= last_run:
            return None
        return occurrence


dash_re = re.compile(r'(\d+)-(\d+)')
every_re = re.compile(r'\*/(\d+)')


class CronSchedule(Schedule):
    """
    Cron-like schedule matched against the wall-clock of the configured
    timezone. See the crontab() helper for the accepted field syntax.

    Occurrences are computed by scanning UTC minute-marks and matching
    the corresponding local wall-clock time, which means:

    * wall-clock times skipped by DST never produce an occurrence,
    * wall-clock times repeated by DST produce a single occurrence.
    """
    def __init__(self, minute='*', hour='*', day='*', month='*',
                 day_of_week='*', strict=False, tz=None, start=None,
                 end=None):
        super(CronSchedule, self).__init__(tz=tz, start=start, end=end)
        validation = (
            ('m', month, range(1, 13)),
            ('d', day, range(1, 32)),
            ('w', day_of_week, range(8)),  # 0-6, but also 7 for Sunday.
            ('H', hour, range(24)),
            ('M', minute, range(60))
        )
        cron_settings = []

        for (date_str, value, acceptable) in validation:
            settings = set([])

            if isinstance(value, int):
                value = str(value)

            for piece in value.split(','):
                if piece == '*':
                    settings.update(acceptable)
                    continue

                if piece.isdigit():
                    piece = int(piece)
                    if piece not in acceptable:
                        raise ValueError('%d is not a valid input' % piece)
                    elif date_str == 'w':
                        piece %= 7
                    settings.add(piece)
                    continue

                dash_match = dash_re.match(piece)
                if dash_match:
                    lhs, rhs = map(int, dash_match.groups())
                    if lhs not in acceptable or rhs not in acceptable:
                        raise ValueError('%s is not a valid input' % piece)
                    elif date_str == 'w':
                        lhs %= 7
                        rhs %= 7
                    settings.update(range(lhs, rhs + 1))
                    continue

                # Handle stuff like */3, */6.
                every_match = every_re.match(piece)
                if every_match:
                    if date_str == 'w':
                        raise ValueError('Cannot perform this kind of '
                                         'matching on day-of-week.')
                    interval = int(every_match.groups()[0])
                    settings.update(acceptable[::interval])
                    continue

                # Older versions of Huey would, at this point, ignore the
                # unmatched piece.
                if strict:
                    raise ValueError('%s is not a valid input' % piece)

            cron_settings.append(sorted(list(settings)))

        self.cron_settings = cron_settings

    def __repr__(self):
        return '<CronSchedule %s tz=%s>' % (self.cron_settings, self.tz)

    def _matches(self, timestamp):
        # Match the cron fields against the given wall-clock time. The
        # timestamp is expected to already be expressed in the schedule's
        # timezone (its tzinfo, if any, is ignored here).
        _, m, d, H, M, _, w, _, _ = timestamp.timetuple()

        # fix the weekday to be sunday=0
        w = (w + 1) % 7

        for (date_piece, selection) in zip((m, d, w, H, M),
                                           self.cron_settings):
            if date_piece not in selection:
                return False

        return True

    def _wall_clock(self, instant, default_tz=None):
        # Convert an aware UTC instant into the wall-clock time used for
        # matching cron fields.
        tz = self.tz if self.tz is not None else default_tz
        if tz is None:
            # System local timezone (handles DST correctly per-date).
            return instant.astimezone()
        return instant.astimezone(tz)

    def validate_datetime(self, timestamp):
        if is_naive(timestamp):
            # Legacy behavior: match the fields of the naive timestamp
            # as-is. For bounds-checking the timestamp is interpreted in
            # the schedule's timezone (default UTC).
            wall = timestamp
            ts = to_utc(timestamp, self.tz or UTC)
        else:
            ts = timestamp.astimezone(UTC)
            if self.tz is not None:
                wall = timestamp.astimezone(self.tz)
            else:
                wall = timestamp
        return self._in_bounds(ts) and self._matches(wall)

    def initial_occurrence(self, now, default_tz=None):
        if not self._in_bounds(now):
            return None
        if self._matches(self._wall_clock(now, default_tz)):
            return minute_floor(now)
        return None

    def due_occurrence(self, last_run, now, default_tz=None):
        hi = now
        if self.end is not None and hi > self.end:
            hi = self.end
        if last_run is None:
            mark = minute_floor(hi)
        elif last_run >= hi:
            # Nothing is due; this also covers a system clock that was
            # rolled back to before the last recorded run.
            return None
        else:
            mark = minute_floor(last_run) + ONE_MINUTE

        # Scan UTC minute-marks, matching the local wall-clock of each.
        # Scanning the UTC timeline (rather than local wall-clock time)
        # means wall-clock times skipped by DST simply never appear, and
        # repeated wall-clock times are de-duplicated using the recently
        # seen wall minutes (DST folds occur one hour apart, so a
        # three-hour window is generous).
        best = None
        recent = {}
        while mark <= hi:
            wall = self._wall_clock(mark, default_tz)
            if self._matches(wall):
                wall_key = wall.replace(tzinfo=None)
                if wall_key not in recent and self._in_bounds(mark):
                    best = mark
                    recent[wall_key] = mark
                    if len(recent) > 500:
                        cutoff = mark - datetime.timedelta(hours=3)
                        recent = dict((k, v) for k, v in recent.items()
                                      if v >= cutoff)
            mark += ONE_MINUTE

        return best


def interval(every, tz=None, start=None, end=None):
    """
    Create a schedule that fires every N seconds (or timedelta). Because
    the occurrences are anchored in UTC they are unaffected by timezone
    and DST changes.
    """
    return IntervalSchedule(every, tz=tz, start=start, end=end)
