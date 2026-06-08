"""Ready-made actors solving common problems."""

from tractor.actors.pool import WorkerPool, Submit, TrySubmit, Reservation

__all__ = ["WorkerPool", "Submit", "TrySubmit", "Reservation"]
