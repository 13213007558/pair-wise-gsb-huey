"""Root pytest configuration.

Allows the test-suite to be collected in sandboxes without a live Redis
server: ``huey.tests.test_storage`` connects at import time (class-level
``skipIf``), so the guarded module is executed with ``Redis.info`` patched to
report version 0, which causes the live-Redis test classes to be skipped.
Protocol-level Redis behavior is covered with an in-process fake client.

When a real Redis is reachable nothing is changed.
"""

import sys


def _redis_available():
    try:
        from redis import Redis
        Redis().info()
        return True
    except Exception:
        return False


if not _redis_available():
    import importlib.abc
    import importlib.machinery

    class _GuardedTestStorageFinder(importlib.abc.MetaPathFinder,
                                    importlib.abc.Loader):
        _target = 'huey.tests.test_storage'

        def find_spec(self, fullname, path=None, target=None):
            if fullname != self._target:
                return None
            real = importlib.machinery.PathFinder().find_spec(fullname, path)
            self._path = real.origin
            real.loader = self
            return real

        def create_module(self, spec):
            return None

        def exec_module(self, module):
            from redis import Redis
            orig_info = Redis.info
            Redis.info = lambda self: {'redis_version': '0.0.0'}
            try:
                with open(self._path, 'rb') as fh:
                    code = compile(fh.read(), self._path, 'exec')
                exec(code, module.__dict__)
            finally:
                Redis.info = orig_info

    sys.meta_path.insert(0, _GuardedTestStorageFinder())
