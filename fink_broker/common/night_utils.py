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
"""Resolve the observing night shared by the streaming jobs.

For a permanent daily production run driven by ScheduledSparkApplication, the
observing night cannot be frozen at Helm-templating time: the same template is
re-instantiated on each cron tick without a per-run date. The night must
therefore be deduced from the current UTC time at runtime.

Only the *policy* (current vs n-1 offset, UTC rollover hour) belongs in
configuration; the date itself is a pure function of ``now``, so it is never
stored in a ConfigMap.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional

from fink_broker.common.tester import regular_unit_tests


def get_night(
    explicit_night: str = "",
    offset_hours: int = 0,
    now: Optional[datetime] = None,
) -> str:
    """Resolve the observing night label (YYYYMMDD).

    A non-empty ``explicit_night`` is returned unchanged (pinned night:
    backfill, rerun of a failed night, or CI). Otherwise the night is deduced
    from the current UTC time following the production policy. This single
    function is shared by stream2raw, raw2science and distribution so the three
    jobs always agree on the same night.

    The policy is a single ``offset_hours`` subtracted from ``now`` before the
    UTC date is extracted. It encodes both the ZTF topic rollover (sub-day
    shift, e.g. 12 for a noon-UTC boundary) and the night selection (24 per day
    back, e.g. 24 for the previous complete night), since both are the same
    time subtraction:

    - 0  -> current night, midnight-UTC rollover (live)
    - 12 -> current night, noon-UTC rollover
    - 24 -> previous complete night (n-1)
    - 36 -> previous night with a noon-UTC rollover

    Parameters
    ----------
    explicit_night: str
        Explicit night in YYYYMMDD format. An empty string means "deduce".
    offset_hours: int
        Hours subtracted from ``now`` before the UTC date is extracted.
    now: datetime, optional
        Reference time (timezone-aware, UTC). Defaults to the current UTC time.
        Mainly used for deterministic testing.

    Returns
    -------
    night: str
        The night label in YYYYMMDD format.

    Examples
    --------
    An explicit night always wins (the policy argument is ignored):
    >>> get_night("20240101")
    '20240101'
    >>> get_night("20240101", offset_hours=24)
    '20240101'

    Otherwise the night is deduced from ``now``:
    >>> from datetime import datetime, timezone
    >>> ref = datetime(2024, 3, 15, 6, 0, tzinfo=timezone.utc)
    >>> get_night(now=ref)
    '20240315'

    An offset of 24 hours selects the previous complete night (n-1):
    >>> get_night(offset_hours=24, now=ref)
    '20240314'

    A noon-UTC rollover keeps a pre-noon run on the previous calendar day:
    >>> get_night(offset_hours=12, now=ref)
    '20240314'
    """
    if explicit_night:
        return explicit_night

    if now is None:
        now = datetime.now(timezone.utc)

    reference = now - timedelta(hours=offset_hours)
    return reference.strftime("%Y%m%d")


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
