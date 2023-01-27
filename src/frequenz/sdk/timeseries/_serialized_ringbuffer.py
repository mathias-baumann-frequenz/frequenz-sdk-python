# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Ringbuffer implementation with serialization support."""

import pickle
from datetime import datetime, timedelta
from os.path import exists
from typing import TypeVar, Sequence, List

import numpy as np

import pyarrow as pa
from pyarrow import parquet

from frequenz.sdk.timeseries._ringbuffer import OrderedRingBuffer, Gap

Container = TypeVar("Container", list, np.ndarray)

VARS_TO_SERIALIZE = (
    "_datetime_oldest",
    "_datetime_newest",
    "_buffer",
    "_gaps",
    "_sampling_period",
    "_time_range",
    "_time_index_alignment",
)

def _serialize_Gap(val: Gap):
    return (val.start, val.end)

def _deserialize_Gap(data):
    return Gap(data["start"], data["end"])

class SerializedRingbuffer(OrderedRingBuffer[Container]):
    """Sorted ringbuffer with serialization support."""

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        buffer: Container,
        sampling_period: timedelta,
        path: str,
        time_index_alignment: datetime = datetime(1, 1, 1),
        load_from_disk: bool = True,
    ) -> None:
        """Initialize the time aware ringbuffer.

        Args:
            buffer: Instance of a buffer container to use internally.
            sampling_period: Timedelta of the desired resampling period.
            path: Path to where the data should be saved.
            time_index_alignment: Arbitary point in time used to align
                timestamped data with the index position in the buffer.
                Used to make the data stored in the buffer align with the
                beginning and end of the buffer borders.
                For example, if the `time_index_alignment` is set to
                "0001-01-01 12:00:00", and the `sampling_period` is set to
                1 hour and the length of the buffer is 24, then the data
                stored in the buffer could correspond to the time range from
                "2022-01-01 12:00:00" to "2022-01-02 12:00:00" (date chosen
                arbitrarily here).
            load_from_disk: Default True, if True, will load the data from disk
                upon start.
        """
        super().__init__(buffer, sampling_period, time_index_alignment)
        self._path = path
        gapType = pa.struct([pa.field("start", pa.timestamp("us")),
                             pa.field("end", pa.timestamp("us"))])

        self._data_schema = pa.schema([pa.field('data', pa.float64())])
        self._gaps_schema = pa.schema([pa.field('gap', gapType)])
        self._metadata_schema = pa.schema([
            pa.field('datetime_oldest', pa.timestamp("us")),
            pa.field('datetime_newest', pa.timestamp("us")),
            pa.field('sampling_period', pa.duration("us")),
            pa.field('time_range', pa.duration("us")),
            pa.field('time_index_alignment', pa.timestamp("us")),
        ])

        if load_from_disk and exists(self._path):
            self.load()

    def dump(self, method: str = "pickle") -> None:
        """Dump data to disk."""
        if method == "parquet":
            parquet.write_table(
                pa.table([self._buffer], self._data_schema),
                f"{self._path}.data", compression='SNAPPY'
            )
            parquet.write_table(
                    pa.table([map(lambda x: (x.start, x.end), self._gaps)], self._gaps_schema),
                f"{self._path}.gaps", compression='SNAPPY'
            )
            parquet.write_table(
                pa.table(
                    [[getattr(self, f"_{name}")] for name in self._metadata_schema.names],
                    self._metadata_schema
                ),
                f"{self._path}.metadata", compression='SNAPPY'
            )
        else:
            with open(self._path, mode="wb+") as fileobj:
                if method == "pickle":
                    pickle.dump(
                        tuple(getattr(self, attr) for attr in VARS_TO_SERIALIZE), fileobj
                    )

    def load(self, method: str = "pickle") -> None:
        """Load data from disk."""
        if method == "parquet":
            self._buffer = pa.parquet.ParquetFile(f"{self._path}.data").read_row_group(0).column(0).to_numpy()

            # x3 slower!
            #self._buffer = parquet.read_table(f"{self._path}.data").column(0).to_numpy()

            gaps_table = parquet.read_table(f"{self._path}.gaps")

            #import pdb; pdb.set_trace()

            self._gaps = [Gap(gap.get(0).as_py(), gap.get(1).as_py()) for gap in gaps_table.column(0)]

            metadata = parquet.read_table(f"{self._path}.metadata")
            for name in metadata.column_names:
                setattr(self, f"_{name}", metadata.column(name).to_numpy()[0])
        else:
            with open(self._path, mode="rb") as fileobj:
                if method == "pickle":
                    for attr, value in zip(VARS_TO_SERIALIZE, pickle.load(fileobj)):
                        setattr(self, attr, value)
