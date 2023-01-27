# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the `SerializedRingbuffer` class."""

from __future__ import annotations

import random
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Any

import numpy as np

from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries._serialized_ringbuffer import SerializedRingbuffer

FIVE_MINUTES = timedelta(minutes=5)
_29_DAYS = 60 * 24 * 29
ONE_MINUTE = timedelta(minutes=1)


def load_dump_test(buffer: SerializedRingbuffer[Any], method: str) -> None:
    """Test ordered ring buffer."""
    size = buffer.maxlen

    random.seed(0)

    # Fill with data so we have something to compare
    # Avoiding .update() because it takes very long for 40k entries
    for i in range(size):
        buffer[i] = i

    # But use update a bit so the timestamp and gaps are initialized
    for i in range(0, size, 100):
        buffer.update(
            Sample(datetime.fromtimestamp(200 + i * FIVE_MINUTES.total_seconds()), i)
        )

    # Make a copy to compare
    before = deepcopy(buffer)

    buffer.dump(method)

    # Overwrite existing data
    buffer[:] = [0] * size

    # pylint: disable=protected-access
    basetime = buffer._datetime_newest

    for i in range(0, size, 50):
        buffer.update(
            Sample(
                basetime + timedelta(seconds=40000 + i * FIVE_MINUTES.total_seconds()),
                i,
            )
        )

    assert list(buffer[:]) != list(before[:])
    # pylint: disable=protected-access
    assert buffer._datetime_oldest != before._datetime_oldest
    # pylint: disable=protected-access
    assert buffer._datetime_newest != before._datetime_newest
    # pylint: disable=protected-access
    assert buffer._gaps != before._gaps

    # pylint: disable=protected-access
    buffer._time_range = timedelta(seconds=200000)
    # pylint: disable=protected-access
    buffer._sampling_period = ONE_MINUTE
    # pylint: disable=protected-access
    buffer._time_index_alignment = datetime.min

    # Load old data
    buffer.load(method)

    assert list(buffer[:]) == list(before[:])
    # pylint: disable=protected-access
    assert buffer._datetime_oldest == before._datetime_oldest
    # pylint: disable=protected-access
    assert buffer._datetime_newest == before._datetime_newest
    # pylint: disable=protected-access
    assert len(buffer._gaps) == len(before._gaps)
    # pylint: disable=protected-access
    assert buffer._gaps == before._gaps
    # pylint: disable=protected-access
    assert buffer._time_range == before._time_range
    # pylint: disable=protected-access
    assert buffer._sampling_period == before._sampling_period
    # pylint: disable=protected-access
    assert buffer._time_index_alignment == before._time_index_alignment


def test_load_dump_short(tmp_path_factory: Any) -> None:
    """Short test to perform loading & dumping."""
    tmpdir = tmp_path_factory.mktemp("load_dump")

    load_dump_test(
        SerializedRingbuffer(
            [0] * int(24 * FIVE_MINUTES.total_seconds()),
            FIVE_MINUTES,
            f"{tmpdir}/test_list.bin",
            datetime(2, 2, 2),
        ), "parquet"
    )

    load_dump_test(
        SerializedRingbuffer(
            np.empty(shape=(24 * int(FIVE_MINUTES.total_seconds()),), dtype=np.float64),
            FIVE_MINUTES,
            f"{tmpdir}/test_array.bin",
            datetime(2, 2, 2),
        ), "parquet"
    )


def test_load_dump(tmp_path_factory: Any) -> None:
    """Test to load/dump 29 days of 1-minute samples."""
    tmpdir = tmp_path_factory.mktemp("load_dump")

    for method in ("pickle", "parquet"):
        load_dump_test(
            SerializedRingbuffer(
                [0] * _29_DAYS, ONE_MINUTE, f"{tmpdir}/test_list_29_{method}.bin", datetime(2, 2, 2)
            ),
            method
        )

        load_dump_test(
            SerializedRingbuffer(
                np.empty(shape=(_29_DAYS,), dtype=np.float64),
                ONE_MINUTE,
                f"{tmpdir}/test_array_29_{method}.bin",
                datetime(2, 2, 2),
            ), method
        )
