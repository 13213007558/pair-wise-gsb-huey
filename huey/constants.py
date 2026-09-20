WORKER_THREAD = 'thread'
WORKER_GREENLET = 'greenlet'
WORKER_PROCESS = 'process'
WORKER_TYPES = (WORKER_THREAD, WORKER_GREENLET, WORKER_PROCESS)


class EmptyData(object):
    pass


# Statuses returned by BaseStorage.chord_vote() when a threshold-chord member
# reports its outcome.
CHORD_PENDING = 'pending'    # Vote recorded, no terminal state reached.
CHORD_CALLBACK = 'callback'  # Threshold met, caller claims the callback.
CHORD_ERROR = 'error'        # Threshold unreachable, caller claims error cb.
CHORD_IGNORED = 'ignored'    # Duplicate vote for an already-counted member.
CHORD_LATE = 'late'          # Chord already reached a terminal state.
