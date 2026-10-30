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
import argparse

import pytest

from fink_broker.common.parser import getargs


def parse(monkeypatch, *argv):
    """Run getargs on the given command line, as an entrypoint would."""
    monkeypatch.setattr("sys.argv", ["job.py", *argv])
    return getargs(argparse.ArgumentParser())


def test_night_pinned(monkeypatch):
    """An explicit -night is taken as is and substituted in the placeholders."""
    args = parse(monkeypatch, "-night", "20240314", "-topic", "ztf_{night}")
    assert args.night == "20240314"
    assert args.topic == "ztf_20240314"


def test_night_is_required(monkeypatch):
    """No -night: argparse rejects the command.

    An omitted night used to default to an empty string, which every job then
    sliced into broken paths ({prefix}/raw/, year=""). The night must be
    stated.
    """
    with pytest.raises(SystemExit) as exc:
        parse(monkeypatch)
    assert exc.value.code == 2


def test_night_is_never_deduced(monkeypatch):
    """-night_offset_hours is gone: the night is always given by the workflow.

    The jobs used to deduce their night from UTC now; every night is now run
    through the WorkflowTemplate, which pins it.
    """
    with pytest.raises(SystemExit) as exc:
        parse(monkeypatch, "-night_offset_hours", "0")
    assert exc.value.code == 2


def test_night_close_hour(monkeypatch):
    """-night_close_hour is the hour the night closes, the next day (UTC).

    It defaults to 0, a night closing at midnight UTC, for the callers that do
    not pass it (production scripts).
    """
    assert parse(monkeypatch, "-night", "20240314").night_close_hour == 0
    args = parse(monkeypatch, "-night", "20240314", "-night_close_hour", "10")
    assert args.night_close_hour == 10


@pytest.mark.parametrize("night", ["", "20241", "202403141", "abcdefgh", "2024-03-14"])
def test_night_pinned_must_be_eight_digits(monkeypatch, night):
    """A malformed -night is refused instead of reaching the paths.

    The value is sliced into topics, buckets and date partitions, so a short
    or non-numeric one would produce a misdirected run rather than an error.
    """
    with pytest.raises(SystemExit) as exc:
        parse(monkeypatch, "-night", night)
    assert exc.value.code == 2
