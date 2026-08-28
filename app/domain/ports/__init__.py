"""Domain ports (protocols) for dependency injection"""

from .blizzard_client import BlizzardClientPort
from .cache import CachePort
from .storage import StoragePort
from .task_queue import EnqueueResult, TaskQueuePort
from .throttle import ThrottlePort

__all__ = [
    "BlizzardClientPort",
    "CachePort",
    "EnqueueResult",
    "StoragePort",
    "TaskQueuePort",
    "ThrottlePort",
]
