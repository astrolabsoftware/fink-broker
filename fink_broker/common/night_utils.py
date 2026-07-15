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
