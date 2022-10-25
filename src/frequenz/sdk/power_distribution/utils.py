"""All helpers used to distribute power.

Copyright
Copyright © 2022 Frequenz Energy-as-a-Service GmbH

License
MIT
"""
from dataclasses import dataclass
from enum import Enum
from typing import NamedTuple, Optional, Set

from frequenz.channels import BidirectionalHandle

from ..microgrid.component_data import BatteryData, InverterData


class InvBatPair(NamedTuple):
    """InvBatPair with inverter and adjacent battery data."""

    battery: BatteryData
    inverter: InverterData


@dataclass
class Request:
    """Request from the user."""

    # How much power to set
    power: int
    # In which batteries the power should be set
    batteries: Set[int]
    # Timeout for the server to respond on the request.
    request_timeout_sec: float = 5.0
    # If True and requested power value is out of bound, then
    # PowerDistributor will decrease the power to match the bounds and
    # distribute only decreased power.
    # If False and the requested power is out of bound, then
    # PowerDistributor will not process this request and send result with status
    # Result.Status.OUT_OF_BOUND.
    adjust_power: bool = True


@dataclass
class Result:
    """Result on distribution request."""

    class Status(Enum):
        """Status of the result."""

        FAILED = 0  # If any request for any battery didn't succeed for any reason.
        SUCCESS = 1  # If all requests for all batteries succeed.
        IGNORED = 2  # If request was dispossessed by newer request with the same set
        # of batteries.
        ERROR = 3  # If any error happened. In this case error_message describes error.
        OUT_OF_BOUND = 4  # When Request.adjust_power=False and the requested power was
        # out of the bounds for specified batteries.

    status: Status  # Status of the request.

    failed_power: float  # How much power failed.

    above_upper_bound: float  # How much power was not used because it was beyond the
    # limits.

    error_message: Optional[
        str
    ] = None  # error_message filled only when status is ERROR


@dataclass
class User:
    """User definitions. Only for internal use."""

    # User id
    user_id: str
    # Channel for the communication
    channel: BidirectionalHandle[Result, Request]
