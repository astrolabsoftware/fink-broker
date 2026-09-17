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

from fink_broker.common.night_utils import get_night, resolve_night_placeholders


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
