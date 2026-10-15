# Copyright 2019-2026 AstroLab Software
# Author: Fabrice Jammes
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Observing night shared by the streaming jobs.

The night is always given to the jobs (-night) by the workflow running it:
nightly run, catch-up or CI. These helpers tell whether it is over, compute
the deadline of a run, and substitute the night in topics and paths.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional

from fink_broker.common.tester import regular_unit_tests


def night_is_closed(night: str, now: datetime, close_hour: int = 0) -> bool:
    """Tell whether ``night`` is over.

    A night is closed from ``close_hour`` UTC on the next day, once its late
    alerts are in: a job processing it can then stop as soon as it has caught
    up, and leave its _DONE marker. Before, the night is open: alerts keep
    arriving, so a quiet spell is not the end of the night, and the job runs
    to its deadline and leaves no marker.

    Parameters
    ----------
    night: str
        Night label in YYYYMMDD format.
    now: datetime
        Reference time (timezone-aware).
    close_hour: int
        Hour (UTC), on the day after the night, from which it is closed.
        Default 0: the night closes at midnight UTC.

    Returns
    -------
    closed: bool
        True from ``close_hour`` UTC on the day after ``night``.

    Examples
    --------
    >>> from datetime import datetime, timezone
    >>> night_is_closed("20261008", datetime(2026, 10, 9, 0, 0, tzinfo=timezone.utc))
    True
    >>> night_is_closed("20261009", datetime(2026, 10, 9, 23, 59, tzinfo=timezone.utc))
    False
    >>> night_is_closed(
    ...     "20261008", datetime(2026, 10, 9, 9, 59, tzinfo=timezone.utc), close_hour=10
    ... )
    False
    """
    closes_at = datetime.strptime(night, "%Y%m%d").replace(
        tzinfo=timezone.utc
    ) + timedelta(days=1, hours=close_hour)
    return now >= closes_at


def check_night(night: str, now: datetime) -> None:
    """Refuse a night which has not started yet.

    Parameters
    ----------
    night: str
        Night label in YYYYMMDD format.
    now: datetime
        Reference time (timezone-aware).

    Raises
    ------
    ValueError
        If ``night`` is after the UTC date of ``now``.

    Examples
    --------
    >>> from datetime import datetime, timezone
    >>> now = datetime(2026, 10, 9, 12, 0, tzinfo=timezone.utc)
    >>> check_night("20261009", now)
    >>> check_night("20261010", now)
    Traceback (most recent call last):
    ...
    ValueError: Night 20261010 is in the future
    """
    if night > now.astimezone(timezone.utc).strftime("%Y%m%d"):
        raise ValueError("Night {} is in the future".format(night))


def get_exit_deadline(
    exit_at: str,
    now: Optional[datetime] = None,
) -> Optional[datetime]:
    """Resolve the UTC instant at which the job must stop.

    Unlike a duration, this deadline is an instant, so a job restarted by the
    operator aims at the same target as its first attempt instead of granting
    itself a fresh budget.

    Two spellings, for the two ways a stop time is decided:

    - ``HH:MM`` -- a time of day in UTC, on the day the job starts. What a
      daily scheduled run wants: one value, valid every day.
    - an ISO 8601 instant -- ``2024-01-01T20:00:00+00:00``, or the same with
      ``Z``, or without an offset (then read as UTC). What a caller computing
      the stop time itself wants, typically to express it in local time, or
      when the run crosses the UTC midnight and a time of day cannot say which
      day is meant.

    The deadline is deliberately allowed to lie in the past: a job starting
    after it has nothing left to do, and the caller is expected to report that
    rather than run a full extra window.

    Parameters
    ----------
    exit_at: str
        ``HH:MM`` (UTC, day of the run) or an ISO 8601 instant. An empty string
        means "no deadline" and returns None.
    now: datetime, optional
        Reference time (timezone-aware, UTC). Defaults to the current UTC time.
        Mainly used for deterministic testing.

    Returns
    -------
    deadline: datetime or None
        The UTC instant at which the job must stop, or None if ``exit_at`` is
        empty.

    Raises
    ------
    ValueError
        If ``exit_at`` is neither a valid HH:MM time of day nor an ISO 8601
        instant.

    Examples
    --------
    >>> from datetime import datetime, timezone
    >>> ref = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    >>> get_exit_deadline("20:00", now=ref)
    datetime.datetime(2024, 1, 1, 20, 0, tzinfo=datetime.timezone.utc)

    The day is the one the job starts on, so a restart later that day keeps
    aiming at the same instant:
    >>> get_exit_deadline("20:00", now=datetime(2024, 1, 1, 18, 30, tzinfo=timezone.utc))
    datetime.datetime(2024, 1, 1, 20, 0, tzinfo=datetime.timezone.utc)

    A job starting past the deadline gets an instant in the past:
    >>> get_exit_deadline("20:00", now=datetime(2024, 1, 1, 23, 0, tzinfo=timezone.utc))
    datetime.datetime(2024, 1, 1, 20, 0, tzinfo=datetime.timezone.utc)

    An ISO instant pins the day too, and ``now`` no longer matters:
    >>> get_exit_deadline("2024-01-02T20:00:00+00:00", now=ref)
    datetime.datetime(2024, 1, 2, 20, 0, tzinfo=datetime.timezone.utc)

    An offset is honoured and normalised to UTC, which is how a local stop time
    is passed in:
    >>> get_exit_deadline("2024-01-02T20:00:00+02:00", now=ref)
    datetime.datetime(2024, 1, 2, 18, 0, tzinfo=datetime.timezone.utc)

    A naive instant is read as UTC:
    >>> get_exit_deadline("2024-01-02T20:00", now=ref)
    datetime.datetime(2024, 1, 2, 20, 0, tzinfo=datetime.timezone.utc)

    An empty value disables the policy:
    >>> get_exit_deadline("") is None
    True
    """
    if not exit_at:
        return None

    try:
        time_of_day = datetime.strptime(exit_at, "%H:%M").time()
    except ValueError:
        pass
    else:
        if now is None:
            now = datetime.now(timezone.utc)
        return now.replace(
            hour=time_of_day.hour,
            minute=time_of_day.minute,
            second=0,
            microsecond=0,
        )

    # Not a time of day: an absolute instant. `Z` is only understood by
    # fromisoformat from Python 3.11 on, so spell it out.
    try:
        instant = datetime.fromisoformat(exit_at.replace("Z", "+00:00"))
    except ValueError as e:
        raise ValueError(
            "Malformed exit_at {}, expected HH:MM (UTC, day of the run) "
            "or an ISO 8601 instant".format(exit_at)
        ) from e

    if instant.tzinfo is None:
        return instant.replace(tzinfo=timezone.utc)
    return instant.astimezone(timezone.utc)


def seconds_until(
    deadline: datetime,
    now: Optional[datetime] = None,
) -> float:
    """Seconds left before ``deadline``, clamped to zero.

    A deadline already in the past yields 0 rather than a negative duration,
    so the result can be handed to ``time.sleep`` unchecked.

    Parameters
    ----------
    deadline: datetime
        Target instant (timezone-aware, UTC).
    now: datetime, optional
        Reference time (timezone-aware, UTC). Defaults to the current UTC time.

    Returns
    -------
    remaining: float
        Seconds between ``now`` and ``deadline``, never negative.

    Examples
    --------
    >>> from datetime import datetime, timezone
    >>> ref = datetime(2024, 1, 1, 18, 0, tzinfo=timezone.utc)
    >>> seconds_until(datetime(2024, 1, 1, 20, 0, tzinfo=timezone.utc), now=ref)
    7200.0

    A past deadline never yields a negative duration:
    >>> seconds_until(datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc), now=ref)
    0.0
    """
    if now is None:
        now = datetime.now(timezone.utc)

    return max(0.0, (deadline - now).total_seconds())


def resolve_night_placeholders(
    value: str,
    night: str,
) -> str:
    """Substitute the ``{night}`` placeholder in a templated value.

    The Kafka topic (``ztf_{night}.*``) and the output prefix/bucket
    (``fink-broker-online-{night}``) carry a ``{night}`` marker so their date
    is resolved in the job at runtime rather than by Helm templating.

    Parameters
    ----------
    value: str
        A string that may contain the ``{night}`` placeholder.
    night: str
        The resolved night label (YYYYMMDD).

    Returns
    -------
    resolved: str
        ``value`` with every ``{night}`` occurrence replaced by ``night``.

    Examples
    --------
    >>> resolve_night_placeholders("ztf_{night}.*", "20240315")
    'ztf_20240315.*'
    >>> resolve_night_placeholders("fink-broker-online-{night}", "20240315")
    'fink-broker-online-20240315'

    Values without the placeholder are returned unchanged:
    >>> resolve_night_placeholders("", "20240315")
    ''
    >>> resolve_night_placeholders("ztf_public_20200101", "20240315")
    'ztf_public_20200101'
    """
    return value.replace("{night}", night)


if __name__ == "__main__":
    """Execute the test suite"""

    # Run the regular test suite
    regular_unit_tests(globals())
