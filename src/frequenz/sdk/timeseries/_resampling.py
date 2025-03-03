# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Timeseries resampler."""

from __future__ import annotations

import asyncio
import logging
import math
from collections import deque
from datetime import datetime, timedelta
from typing import AsyncIterator, Callable, Coroutine, Sequence

from frequenz.channels.util import Timer

from ..util.asyncio import cancel_and_await
from . import Sample

_logger = logging.Logger(__name__)


Source = AsyncIterator[Sample]
"""A source for a timeseries.

A timeseries can be received sample by sample in a streaming way
using a source.
"""

Sink = Callable[[Sample], Coroutine[None, None, None]]
"""A sink for a timeseries.

A new timeseries can be generated by sending samples to a sink.

This should be an `async` callable, for example:

``` python
async some_sink(Sample) -> None:
    ...
```

Args:
    sample (Sample): A sample to be sent out.
"""


ResamplingFunction = Callable[[Sequence[Sample], float], float]
"""Resampling function type.

A resampling function produces a new sample based on a list of pre-existing
samples. It can do "upsampling" when there data rate of the `input_samples`
period is smaller than the `resampling_period_s`, or "downsampling" if it is
bigger.

In general a resampling window is the same as the `resampling_period_s`, and
this function might receive input samples from multiple windows in the past to
enable extrapolation, but no samples from the future (so the timestamp of the
new sample that is going to be produced will always be bigger than the biggest
timestamp in the input data).

Args:
    input_samples (Sequence[Sample]): the sequence of pre-existing samples.
    resampling_period_s (float): the period in seconds (i.e. how ofter a new sample is
        produced.

Returns:
    new_sample (float): The value of new sample produced after the resampling.
"""


# pylint: disable=unused-argument
def average(samples: Sequence[Sample], resampling_period_s: float) -> float:
    """Calculate average of all the provided values.

    Args:
        samples: The samples to apply the average to. It must be non-empty.
        resampling_period_s: The time it passes between resampled data is
            produced (in seconds).

    Returns:
        The average of all `samples` values.
    """
    assert len(samples) > 0, "Average cannot be given an empty list of samples"
    values = list(sample.value for sample in samples if sample.value is not None)
    return sum(values) / len(values)


class SourceStoppedError(RuntimeError):
    """A timeseries stopped producing samples."""

    def __init__(self, source: Source) -> None:
        """Create an instance.

        Args:
            source: The source of the timeseries that stopped producting
                samples.
        """
        super().__init__(f"Timeseries stopped producing samples, source: {source}")
        self.source = source
        """The source of the timeseries that stopped producting samples."""

    def __repr__(self) -> str:
        """Return the representation of the instance.

        Returns:
            The representation of the instance.
        """
        return f"{self.__class__.__name__}({self.source!r})"


class ResamplingError(RuntimeError):
    """An Error ocurred while resampling.

    This error is a container for errors raised by the underlying sources and
    or sinks.
    """

    def __init__(
        self, exceptions: dict[Source, Exception | asyncio.CancelledError]
    ) -> None:
        """Create an instance.

        Args:
            exceptions: A mapping of timeseries source and the exception
                encountered while resampling that timeseries. Note that the
                error could be raised by the sink, while trying to send
                a resampled data for this timeseries, the source key is only
                used to identify the timeseries with the issue, it doesn't
                necessarily mean that the error was raised by the source. The
                underlying exception should provide information about what was
                the actual source of the exception.
        """
        super().__init__(f"Some error were found while resampling: {exceptions}")
        self.exceptions = exceptions
        """A mapping of timeseries source and the exception encountered.

        Note that the error could be raised by the sink, while trying to send
        a resampled data for this timeseries, the source key is only used to
        identify the timeseries with the issue, it doesn't necessarily mean
        that the error was raised by the source. The underlying exception
        should provide information about what was the actual source of the
        exception.
        """

    def __repr__(self) -> str:
        """Return the representation of the instance.

        Returns:
            The representation of the instance.
        """
        return f"{self.__class__.__name__}({self.exceptions=})"


class Resampler:
    """A timeseries resampler.

    In general timeseries [`Source`][frequenz.sdk.timeseries.Source]s don't
    necessarily come at periodic intervals. You can use this class to normalize
    timeseries to produce `Sample`s at regular periodic intervals.

    This class uses
    a [`ResamplingFunction`][frequenz.sdk.timeseries.resampling.ResamplingFunction]
    to produce a new sample from samples received in the past. If there are no
    samples coming to a resampled timeseries for a while, eventually the
    `Resampler` will produce `Sample`s with `None` as value, meaning there is
    no way to produce meaningful samples with the available data.
    """

    def __init__(
        self,
        *,
        resampling_period_s: float,
        resampling_function: ResamplingFunction = average,
        max_data_age_in_periods: float = 3.0,
    ) -> None:
        """Initialize an instance.

        Args:
            resampling_period_s: The time it passes between resampled data
                should be calculated (in seconds).
            max_data_age_in_periods: The maximum age a sample can have to be
                considered *relevant* for resampling purposes, expressed in the
                number of resampling periods. For exapmle is
                `resampling_period_s` is 3 and `max_data_age_in_periods` is 2,
                then data older than `3*2 = 6` secods will be discarded when
                creating a new sample and never passed to the resampling
                function.
            resampling_function: The function to be applied to the sequence of
                *relevant* samples at a given time. The result of the function
                is what is sent as the resampled data.
        """
        self._resampling_period_s = resampling_period_s
        self._max_data_age_in_periods: float = max_data_age_in_periods
        self._resampling_function: ResamplingFunction = resampling_function
        self._resamplers: dict[Source, _StreamingHelper] = {}
        self._timer: Timer = Timer(self._resampling_period_s)

    async def stop(self) -> None:
        """Cancel all receiving tasks."""
        await asyncio.gather(*[helper.stop() for helper in self._resamplers.values()])

    def add_timeseries(self, source: Source, sink: Sink) -> bool:
        """Start resampling a new timeseries.

        Args:
            source: The source of the timeseries to resample.
            sink: The sink to use to send the resampled data.

        Returns:
            `True` if the timeseries was added, `False` if the timeseries was
            not added because there already a timeseries using the provided
            receiver.
        """
        if source in self._resamplers:
            return False

        resampler = _StreamingHelper(
            _ResamplingHelper(
                resampling_period_s=self._resampling_period_s,
                max_data_age_in_periods=self._max_data_age_in_periods,
                resampling_function=self._resampling_function,
            ),
            source,
            sink,
        )
        self._resamplers[source] = resampler
        return True

    def remove_timeseries(self, source: Source) -> bool:
        """Stop resampling the timeseries produced by `source`.

        Args:
            source: The source of the timeseries to stop resampling.

        Returns:
            `True` if the timeseries was removed, `False` if nothing was
                removed (because the a timeseries with that `source` wasn't
                being resampled).
        """
        try:
            del self._resamplers[source]
        except KeyError:
            return False
        return True

    async def resample(self, *, one_shot: bool = False) -> None:
        """Start resampling all known timeseries.

        This method will run forever unless there is an error while receiving
        from a source or sending to a sink (or `one_shot` is used).

        Args:
            one_shot: Wether the resampling should run only for one resampling
                period.

        Raises:
            ResamplingError: If some timseries source or sink encounters any
                errors while receiving or sending samples. In this case the
                timer still runs and the timeseries will keep receiving data.
                The user should remove (and re-add if desired) the faulty
                timeseries from the resampler before calling this method
                again).
        """
        async for timer_timestamp in self._timer:
            results = await asyncio.gather(
                *[r.resample(timer_timestamp) for r in self._resamplers.values()],
                return_exceptions=True,
            )
            exceptions = {
                source: results[i]
                for i, source in enumerate(self._resamplers)
                # CancelledError inherits from BaseException, but we don't want
                # to catch *all* BaseExceptions here.
                if isinstance(results[i], (Exception, asyncio.CancelledError))
            }
            if exceptions:
                raise ResamplingError(exceptions)
            if one_shot:
                break


class _ResamplingHelper:
    """Keeps track of *relevant* samples to pass them to the resampling function.

    Samples are stored in an internal ring buffer. All collected samples that
    are newer than `resampling_period_s * max_data_age_in_periods` seconds are
    considered *relevant* and are passed to the provided `resampling_function`
    when calling the `resample()` method. All older samples are discarded.
    """

    def __init__(
        self,
        *,
        resampling_period_s: float,
        max_data_age_in_periods: float,
        resampling_function: ResamplingFunction,
    ) -> None:
        """Initialize an instance.

        Args:
            resampling_period_s: The time it passes between resampled data
                should be calculated (in seconds).
            max_data_age_in_periods: The maximum age a sample can have to be
                considered *relevant* for resampling purposes, expressed in the
                number of resampling periods. For exapmle is
                `resampling_period_s` is 3 and `max_data_age_in_periods` is 2,
                then data older than 3*2 = 6 secods will be discarded when
                creating a new sample and never passed to the resampling
                function.
            resampling_function: The function to be applied to the sequence of
                relevant samples at a given time. The result of the function is
                what is sent as the resampled data.
        """
        self._resampling_period_s = resampling_period_s
        self._max_data_age_in_periods: float = max_data_age_in_periods
        self._buffer: deque[Sample] = deque()
        self._resampling_function: ResamplingFunction = resampling_function

    def add_sample(self, sample: Sample) -> None:
        """Add a new sample to the internal buffer.

        Args:
            sample: The sample to be added to the buffer.
        """
        self._buffer.append(sample)

    def _remove_outdated_samples(self, threshold: datetime) -> None:
        """Remove samples that are older than the provided time threshold.

        It is assumed that items in the buffer are in a sorted order (ascending order
        by timestamp).

        The removal works by traversing the buffer starting from the oldest sample
        (smallest timestamp) and comparing sample's timestamp with the threshold.
        If the sample's threshold is smaller than `threshold`, it means that the
        sample is outdated and it is removed from the buffer. This continues until
        the first sample that is with timestamp greater or equal to `threshold` is
        encountered, then buffer is considered up to date.

        Args:
            threshold: samples whose timestamp is older than the threshold are
                considered outdated and should be remove from the buffer
        """
        while self._buffer:
            sample: Sample = self._buffer[0]
            if sample.timestamp > threshold:
                return

            self._buffer.popleft()

    def resample(self, timestamp: datetime) -> Sample:
        """Generate a new sample based on all the current *relevant* samples.

        Args:
            timestamp: The timestamp to be used to calculate the new sample.

        Returns:
            A new sample generated by calling the resampling function with all
                the current *relevant* samples in the internal buffer, if any.
                If there are no *relevant* samples, then the new sample will
                have `None` as `value`.
        """
        threshold = timestamp - timedelta(
            seconds=self._max_data_age_in_periods * self._resampling_period_s
        )
        self._remove_outdated_samples(threshold=threshold)

        value = (
            None
            if not self._buffer
            else self._resampling_function(self._buffer, self._resampling_period_s)
        )
        return Sample(timestamp, value)


class _StreamingHelper:
    """Resample data coming from a source, sending the results to a sink."""

    def __init__(
        self,
        helper: _ResamplingHelper,
        source: Source,
        sink: Sink,
    ) -> None:
        """Initialize an instance.

        Args:
            helper: The helper instance to use to resample incoming data.
            source: The source to use to get the samples to be resampled.
            sink: The sink to use to send the resampled data.
        """
        self._helper: _ResamplingHelper = helper
        self._source: Source = source
        self._sink: Sink = sink
        self._receiving_task: asyncio.Task = asyncio.create_task(
            self._receive_samples()
        )

    async def stop(self) -> None:
        """Cancel the receiving task."""
        await cancel_and_await(self._receiving_task)

    async def _receive_samples(self) -> None:
        """Pass received samples to the helper.

        This method keeps running until the source stops (or fails with an
        error).
        """
        async for sample in self._source:
            if sample.value is not None and not math.isnan(sample.value):
                self._helper.add_sample(sample)

    async def resample(self, timestamp: datetime) -> None:
        """Calculate a new sample for the passed `timestamp` and send it.

        The helper is used to calculate the new sample and the sender is used
        to send it.

        Args:
            timestamp: The timestamp to be used to calculate the new sample.

        Raises:
            SourceStoppedError: If the source stopped sending samples.
            Exception: if there was any error while receiving from the source
                or sending to the sink.

                If the error was in the source, then this helper will stop
                working, as the internal task to receive samples will stop due
                to the exception. Any subsequent call to `resample()` will keep
                raising the same exception.

                If the error is in the sink, the receiving part will continue
                working while this helper is alive.

        # noqa: DAR401 recv_exception
        """
        if self._receiving_task.done():
            if recv_exception := self._receiving_task.exception():
                raise recv_exception
            raise SourceStoppedError(self._source)

        await self._sink(self._helper.resample(timestamp))
