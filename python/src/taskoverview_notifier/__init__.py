"""Report progress of long-running jobs to the TaskOverview Übersicht widget."""

from ._spawn import ServerUnavailable
from .client import Job, Notifier, SyncJob, SyncNotifier

__version__ = "0.2.0"

__all__ = ["Job", "Notifier", "ServerUnavailable", "SyncJob", "SyncNotifier", "__version__"]
