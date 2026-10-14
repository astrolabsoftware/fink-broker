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
    night_is_closed,
    resolve_night_placeholders,
)


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


def test_past_night_is_closed():
    """A night before today's UTC date is closed, from the first minute."""
    now = datetime(2026, 10, 9, 0, 0, tzinfo=timezone.utc)
    assert night_is_closed("20261008", now)


def test_today_night_is_open():
    """Today's UTC night stays open until the UTC midnight."""
    now = datetime(2026, 10, 9, 23, 59, tzinfo=timezone.utc)
    assert not night_is_closed("20261009", now)


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
