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
from datetime import datetime, timezone

import pytest

from fink_broker.common.night_utils import (
    check_night,
    get_night,
    night_is_closed,
    resolve_night_placeholders,
)


def test_explicit_night_wins():
    """An explicit night is returned unchanged, the policy arg is ignored."""
    assert get_night("20240101") == "20240101"
    assert get_night("20240101", offset_hours=36) == "20240101"


def test_deduce_current_night():
    """With no offset, the night is the current UTC date."""
    ref = datetime(2024, 3, 15, 6, 0, tzinfo=timezone.utc)
    assert get_night(now=ref) == "20240315"


def test_deduce_previous_night():
    """An offset of 24 hours selects the previous complete night (n-1)."""
    ref = datetime(2024, 3, 15, 6, 0, tzinfo=timezone.utc)
    assert get_night(offset_hours=24, now=ref) == "20240314"


def test_rollover_before_hour_keeps_previous_day():
    """A pre-noon run with a noon-UTC rollover resolves to the previous day."""
    ref = datetime(2024, 3, 15, 6, 0, tzinfo=timezone.utc)
    assert get_night(offset_hours=12, now=ref) == "20240314"


def test_rollover_after_hour_keeps_current_day():
    """A post-noon run with a noon-UTC rollover resolves to the current day."""
    ref = datetime(2024, 3, 15, 18, 0, tzinfo=timezone.utc)
    assert get_night(offset_hours=12, now=ref) == "20240315"


def test_offset_crosses_month_boundary():
    """Shifting back across midnight handles month boundaries."""
    ref = datetime(2024, 3, 1, 2, 0, tzinfo=timezone.utc)
    assert get_night(offset_hours=12, now=ref) == "20240229"


def test_previous_night_with_noon_rollover():
    """36 hours = n-1 combined with a noon-UTC rollover."""
    ref = datetime(2024, 3, 15, 6, 0, tzinfo=timezone.utc)
    # -36h -> 2024-03-13 18:00
    assert get_night(offset_hours=36, now=ref) == "20240313"


def test_resolve_placeholder_topic():
    """The {night} placeholder is substituted in a topic pattern."""
    assert resolve_night_placeholders("ztf_{night}.*", "20240315") == "ztf_20240315.*"


def test_resolve_placeholder_bucket():
    """The {night} placeholder is substituted in an output prefix/bucket."""
    assert (
        resolve_night_placeholders("fink-broker-online-{night}", "20240315")
        == "fink-broker-online-20240315"
    )


def test_resolve_placeholder_noop():
    """Values without the placeholder are returned unchanged."""
    assert resolve_night_placeholders("", "20240315") == ""
    assert (
        resolve_night_placeholders("ztf_public_20200101", "20240315")
        == "ztf_public_20200101"
    )


@pytest.mark.parametrize(
    "now, closed",
    [
        # Today's night, while its alerts arrive
        (datetime(2026, 10, 8, 18, 0, tzinfo=timezone.utc), False),
        # Some nights still receive alerts until 22:00-03:00 UTC the next day
        (datetime(2026, 10, 9, 0, 30, tzinfo=timezone.utc), False),
        (datetime(2026, 10, 9, 9, 59, tzinfo=timezone.utc), False),
        # Closed from 10:00 UTC the next day on
        (datetime(2026, 10, 9, 10, 0, tzinfo=timezone.utc), True),
        (datetime(2026, 10, 11, 6, 0, tzinfo=timezone.utc), True),
    ],
)
def test_night_closes_at_10_utc_the_next_day(now, closed):
    """A night is closed once its late alerts are in, by default 10:00 UTC J+1."""
    assert night_is_closed("20261008", now) is closed


def test_night_close_hour_is_a_parameter():
    """The closing hour, on the day after the night, can be changed."""
    now = datetime(2026, 10, 9, 0, 0, tzinfo=timezone.utc)
    assert night_is_closed("20261008", now, close_hour=0)
    assert not night_is_closed("20261008", now, close_hour=1)


def test_future_night_is_refused():
    """A night after today's UTC date has no alert yet: refused at startup."""
    now = datetime(2026, 10, 9, 12, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="Night 20261010 is in the future"):
        check_night("20261010", now)


@pytest.mark.parametrize("night", ["20261008", "20261009"])
def test_past_or_today_night_is_accepted(night):
    """A closed night or today's open night can be processed."""
    now = datetime(2026, 10, 9, 12, 0, tzinfo=timezone.utc)
    check_night(night, now)
