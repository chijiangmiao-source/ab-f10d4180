"""Near-Earth Orbit radiation-monitoring service.

Two independent probes submit dose readings that may arrive out of order and
may be retransmitted. Results are read back per fixed five-minute alarm
window. The engine advances a low watermark from the two probes' observed
progress and seals windows in a single durable transaction; once sealed, a
window can never be rewritten by late packets.
"""

__all__ = ["ALARM_WINDOW_SECONDS"]

#: Length of an alarm window (five minutes). Windows are fixed, epoch aligned.
ALARM_WINDOW_SECONDS = 300
