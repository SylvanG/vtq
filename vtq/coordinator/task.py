from vtq.model import Task


def is_queued(task: Task) -> bool:
    return task.status < 50


def is_unstarted(task: Task) -> bool:
    return task.status == 0


def is_pending(task: Task) -> bool:
    return task.status == 1


def is_wip(task: Task) -> bool:
    return task.status >= 50 and not is_ended(task)


def is_ended(task: Task) -> bool:
    return task.status >= 100


def is_succeeded(task: Task) -> bool:
    return task.status == 100


def is_failed(task: Task) -> bool:
    return task.status > 100


Task.is_queued = is_queued
Task.is_unstarted = is_unstarted
Task.is_pending = is_pending
Task.is_ended = is_ended
Task.is_wip = is_wip
