"""
Tools for expiring task results and result-store metadata.

This module implements a small policy engine used to determine how long
different categories of result-store entries should be retained, along with
helpers for encoding/decoding the metadata records the storage layer keeps
alongside each result.
"""
import datetime
import json


# Categories of result-store entries, each of which may be assigned its own
# TTL (time-to-live, in seconds):
#
# * complete - return values of tasks that finished successfully. These are
#   typically "business results" and are usually retained the longest.
# * error - error results for tasks that failed and will not be retried.
# * retry - error results for tasks that failed but will be retried. These
#   are short-term debugging information and usually expire quickly.
# * group - group/chord bookkeeping metadata (member lists, summaries).
# * revoked - revocation markers written by Huey.revoke()/revoke_all().
# * pending - entries whose write is in-progress or whose metadata is
#   missing/unrecognized. These are never considered expired unless an
#   explicit TTL is configured for the category.
RESULT_CATEGORIES = ('complete', 'error', 'retry', 'group', 'revoked',
                     'pending')


def normalize_ttl(value, param='ttl'):
    """
    Normalize a TTL value to seconds (float) or None.

    * None -> never expires.
    * 0 -> expires immediately (eligible for deletion on the next cleanup).
    * positive number / timedelta -> TTL in seconds.
    * negative values raise ValueError.
    """
    if value is None:
        return None
    if isinstance(value, datetime.timedelta):
        value = value.total_seconds()
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError('%s must be None, a number of seconds, or a '
                         'datetime.timedelta, got %r' % (param, value))
    if value < 0:
        raise ValueError('%s must not be negative, got %r' % (param, value))
    return float(value)


def _normalize_rule(value, param='ttl'):
    """
    Normalize a TTL rule, which may be a single TTL (applied to all
    categories) or a dict mapping category -> TTL.
    """
    if isinstance(value, dict):
        accum = {}
        for key, ttl in value.items():
            if key != 'default' and key not in RESULT_CATEGORIES:
                raise ValueError('unknown result category %r in %s' %
                                 (key, param))
            accum[key] = normalize_ttl(ttl, param)
        return accum
    return {'default': normalize_ttl(value, param)}


class ResultExpirationPolicy(object):
    """
    Policy that determines the TTL for a given category of result-store
    entry, optionally overridden on a per-task basis.

    :param default: TTL applied when nothing more specific matches.
    :param categories: dict mapping category name -> TTL.
    :param tasks: dict mapping task name -> TTL (or dict of category -> TTL).
    """
    def __init__(self, default=None, categories=None, tasks=None):
        self.default = normalize_ttl(default, 'default')
        self.categories = {}
        for category, ttl in (categories or {}).items():
            if category not in RESULT_CATEGORIES:
                raise ValueError('unknown result category %r' % category)
            self.categories[category] = normalize_ttl(ttl, category)
        self.tasks = {}
        for task_name, rule in (tasks or {}).items():
            self.tasks[task_name] = _normalize_rule(
                rule, 'ttl for task %r' % task_name)

    @classmethod
    def from_config(cls, config):
        """
        Build a policy from the user-provided configuration, which may be:

        * None - nothing expires (the default).
        * a number of seconds or timedelta - single TTL for all categories.
        * a dict with optional keys:
            - "default": fallback TTL.
            - any category name ("complete", "error", "retry", "group",
              "revoked", "pending"): TTL for that category.
            - "tasks": dict mapping task name -> TTL (or nested dict of
              category -> TTL).
        """
        if config is None:
            return cls()
        if isinstance(config, dict):
            config = dict(config)
            tasks = config.pop('tasks', None)
            default = config.pop('default', None)
            unknown = set(config).difference(RESULT_CATEGORIES)
            if unknown:
                raise ValueError('unknown result categories: %s' %
                                 ', '.join(sorted(unknown)))
            return cls(default=default, categories=config, tasks=tasks)
        return cls(default=config)

    def _resolve(self, category, task_name, task_config):
        # Precedence, highest first:
        # 1. per-task rule passed explicitly (task class "result_ttl" attr).
        # 2. per-task rule from the policy "tasks" mapping.
        # 3. per-category rule from the policy.
        # 4. policy default.
        rules = []
        if task_config is not None:
            rules.append(_normalize_rule(task_config, 'result_ttl'))
        if task_name:
            if task_name in self.tasks:
                rules.append(self.tasks[task_name])
            short_name = task_name.rsplit('.', 1)[-1]
            if short_name != task_name and short_name in self.tasks:
                rules.append(self.tasks[short_name])
        for rule in rules:
            if category in rule:
                return rule[category]
            if 'default' in rule:
                return rule['default']
        if category in self.categories:
            return self.categories[category]
        return self.default

    def ttl_for(self, category, task_name=None, task_config=None):
        """
        Return the TTL in seconds for the given category (and optional task),
        or None if entries of this kind never expire.
        """
        if category not in RESULT_CATEGORIES:
            raise ValueError('unknown result category %r' % category)
        return self._resolve(category, task_name, task_config)

    def is_expired(self, category, ts, now, task_name=None, task_config=None):
        """
        Return whether an entry written at timestamp "ts" is expired as of
        timestamp "now". Entries with no TTL (None) or no timestamp never
        expire. A TTL of 0 expires immediately.
        """
        ttl = self.ttl_for(category, task_name, task_config)
        if ttl is None or ts is None:
            return False
        return ts + ttl <= now

    def __repr__(self):
        return ('ResultExpirationPolicy(default=%r, categories=%r, '
                'tasks=%r)' % (self.default, self.categories, self.tasks))


def encode_result_meta(meta):
    """
    Encode a result-metadata dict to its canonical (JSON) text form. The
    encoding is deterministic so that a previously-read value can be used
    for atomic compare-and-delete operations.
    """
    return json.dumps(meta, sort_keys=True, separators=(',', ':'))


def decode_result_meta(raw):
    """
    Decode a raw metadata value. Returns None if the value cannot be
    decoded (e.g. corrupt or written by something else).
    """
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode('utf8')
    try:
        meta = json.loads(raw)
    except (ValueError, TypeError):
        return None
    return meta if isinstance(meta, dict) else None
