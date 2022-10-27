"""
Utils for BatteryStatus module.

Copyright
Copyright © 2022 Frequenz Energy-as-a-Service GmbH

License
MIT
"""
import datetime
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict


class BatteryResult(Enum):
    """Possible battery results returned by PowerDistributor."""

    # Power for that battery was successfully set.
    SUCCESS = 0
    # Setting power for that battery failed, because it was out of range.
    OUT_OF_RANGE = 1
    # Battery failed with error that was printed to the logs.
    ERROR = 2
    # Battery didn't respond for `set_power` operation in time. It is unknown if power
    # was set or not.
    UNKNOWN_RESULT = 3
    # Battery was not used. PowerDistributor stopped receiving data for that component.
    UNUSED = 4


@dataclass
class TimeoutParam:
    """Set timeout for component, and track when it is finished."""

    start_time: datetime.datetime
    duration_s: float = 1
    max_timeout_duration_s = 30
    expiration_time: datetime.datetime = field(init=False)

    def __post_init__(self) -> None:
        """Create parameters based on the input."""
        self.expiration_time = self.start_time + datetime.timedelta(
            seconds=self.duration_s
        )

    def update(self, now: datetime.datetime) -> float:
        """Update timeout.

        Next timeout will be twice longer then the previous run.
        Maximum duration time for the timeout is self.max_timeout_duration_s.

        Args:
            now: where the time for timeout should finish.

        Returns:
            If timeout was updated, return for how long it was set.
        """
        if self.expiration_time < now:
            self.duration_s = min(self.duration_s * 2, self.max_timeout_duration_s)
            self.expiration_time = now + datetime.timedelta(seconds=self.duration_s)
            return self.duration_s
        return 0

    def is_expired(self, now: datetime.datetime) -> bool:
        """Check if the timeout expired.

        Args:
            now: Current time, to compare with timeout expiration time.

        Returns:
            True if timeout expired, False otherwise.
        """
        return self.expiration_time < now


class LastResponse:
    """Holds logic for information on last response from `set_power` method.

    Check the last response and block component for some time if it failed.
    """

    invalid_battery_result = {BatteryResult.ERROR, BatteryResult.UNKNOWN_RESULT}

    def __init__(self) -> None:
        """Create LastResponse class."""
        self._components: Dict[int, TimeoutParam] = {}

    def update(
        self, component_id: int, result: BatteryResult, now: datetime.datetime
    ) -> float:
        """Update component.

        Args:
            component_id: Id of the component
            result: Result of the last operation on that battery
            now: current timestamp

        Returns:
            How long (in seconds) timestamp was set. 0 If timestamp was not set.
        """
        if result in LastResponse.invalid_battery_result:
            if component_id in self._components:
                return self._components[component_id].update(now)

            self._components[component_id] = TimeoutParam(now)
            return self._components[component_id].duration_s

        del self._components[component_id]
        return 0

    def is_correct(self, component_id: int, now: datetime.datetime) -> bool:
        """Check if based on last status, component can be used.

        Return if last response for given component id was correct and component is not blocked.

        Args:
            component_id: component id
            now: current timestamp

        Returns:
            True if component can be used, false otherwise.
        """
        return component_id not in self._components or self._components[
            component_id
        ].is_expired(now)


@dataclass
class LastStatus:
    """Holds if the last status for the battery was correct, or not.

    Attributes:
    last_message_received: If message received through the channel.
    last_status_correct: If recent message has correct status.
    last_message_not_outdated: If recent message was outdated.
    last_error_code_not_critical: If last error code was not critical.
    """

    last_message_received: bool = True
    last_status_correct: bool = True
    last_message_not_outdated: bool = False
    last_error_code_not_critical: bool = False
