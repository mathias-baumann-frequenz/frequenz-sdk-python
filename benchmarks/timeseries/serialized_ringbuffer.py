# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests for the `SerializedRingbuffer` class."""

from __future__ import annotations

import fnmatch
import os
import random
import time
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Any

import numpy as np

from frequenz.sdk.timeseries import Sample
from frequenz.sdk.timeseries._serialized_ringbuffer import SerializedRingbuffer


def delete_files_with_prefix(prefix):
    for file in os.listdir():
        if fnmatch.fnmatch(file, prefix + "*"):
            os.remove(file)


FIVE_MINUTES = timedelta(minutes=5)
_29_DAYS = 60 * 24 * 29
ONE_MINUTE = timedelta(minutes=1)


def benchmark_serialization(ringbuffer, method, iterations):
    total = 0
    for _ in range(iterations):
        start = time.time()
        ringbuffer.dump(method)
        ringbuffer.load(method)
        end = time.time()
        total += end - start
        # delete_files_with_prefix("ringbuffer.pkl")

    return total / iterations


size = 4000_000
iterations = 10

ringbuffer = SerializedRingbuffer(
    np.arange(0, size, dtype=np.float64), timedelta(minutes=5), "ringbuffer.pkl"
)

print("size:", size)
print("iterations:", iterations)
# But use update a bit so the timestamp and gaps are initialized
for i in range(0, size, 10000):
    ringbuffer.update(
        Sample(datetime.fromtimestamp(200 + i * FIVE_MINUTES.total_seconds()), i)
    )

print(
    "Avg time for Pickle dump/load:  ",
    benchmark_serialization(ringbuffer, "pickle", iterations),
    "s",
)
print(
    "Avg time for Parquet dump/load: ",
    benchmark_serialization(ringbuffer, "parquet", iterations),
    "s",
)
