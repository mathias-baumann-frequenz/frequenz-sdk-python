"""
Definition of tool that hold status of the batteries pool.

Copyright
Copyright © 2022 Frequenz Energy-as-a-Service GmbH

License
MIT
"""
import asyncio
import datetime
import logging
from typing import Dict, Set

import frequenz.api.microgrid.battery_pb2 as battery_pb
import frequenz.api.microgrid.common_pb2 as common_pb
import frequenz.api.microgrid.inverter_pb2 as inverter_pb
import pytz
from frequenz.channels import Peekable, Receiver

from ..microgrid import ComponentCategory, microgrid_api
from ..microgrid.component_data import BatteryData, InverterData
from .utils import BatteryResult, LastResponse, LastStatus

_logger = logging.getLogger(__name__)


class BatteryStatus:
    """Check the status of each battery and make decision which should be used."""

    battery_invalid_relay: Set[battery_pb.RelayState.ValueType] = {
        battery_pb.RelayState.RELAY_STATE_OPENED
    }
    battery_invalid_state: Set[battery_pb.ComponentState.ValueType] = {
        battery_pb.ComponentState.COMPONENT_STATE_ERROR
    }
    inverter_invalid_state: Set[inverter_pb.ComponentState.ValueType] = {
        inverter_pb.ComponentState.COMPONENT_STATE_UNSPECIFIED,
        inverter_pb.ComponentState.COMPONENT_STATE_UNAVAILABLE,
    }

    def __init__(self, battery_pool: Set[int], max_inactive_duration_s: float) -> None:
        """Create object instance.

        Args:
            battery_pool (Set[int]): Pool of batteries that should be tracked.
            max_inactive_duration_s (float): If component stopped sending data, then
                this is the maximum time when its last message should be considered as
                valid. After that time, component would be used until it starts sending
                data.
        """
        self._max_inactive_duration: float = max_inactive_duration_s
        self._bat_inv_map: Dict[int, int] = self._get_component_pairs(battery_pool)

        # Channels for battery and inverter data.
        self._battery_receivers: Dict[int, Peekable[BatteryData]] = {}
        self._inverter_receivers: Dict[int, Peekable[InverterData]] = {}

        # To check last status
        self._last_response = LastResponse()
        self._last_status: Dict[int, LastStatus] = {}
        self._last_returned_unknown_batteries: bool = False

    async def run(self) -> None:
        """Subscribe for the component data.

        This method needs to be called before first use of BatteryStatus instance.
        """
        self._battery_receivers = {}
        self._inverter_receivers = {}
        self._last_status = {}
        self._last_returned_unknown_batteries = False

        api = microgrid_api.get()
        microgrid_client = api.microgrid_api_client

        for battery_id, inverter_id in self._bat_inv_map.items():
            bat_recv: Receiver[BatteryData] = await microgrid_client.battery_data(
                battery_id
            )
            self._battery_receivers[battery_id] = bat_recv.into_peekable()

            inv_recv: Receiver[InverterData] = await microgrid_client.inverter_data(
                inverter_id
            )
            self._inverter_receivers[inverter_id] = inv_recv.into_peekable()

            self._last_status[battery_id] = LastStatus()

        # Wait for the first component data
        await asyncio.sleep(2)

    def update_battery_result(self, battery_results: Dict[int, BatteryResult]) -> None:
        """Update information about th result of the recent `set_power` method.

        Args:
            battery_results (Dict[int, BatteryResult]): result for each battery.

        Raises:
            RuntimeError: If this method was called before `run` method.
        """
        if len(self._battery_receivers) == 0:
            raise RuntimeError(
                "User didn't subscribed for the component data. Call `run` method "
                "before the first use."
            )

        now = datetime.datetime.now()
        for battery_id, result in battery_results.items():
            timeout_duration = self._last_response.update(battery_id, result, now)
            if timeout_duration > 0:
                _logger.info(
                    "Battery %d didn't succeed in last call. Set it as broken for %f sec",
                    battery_id,
                    timeout_duration,
                )

    def get_working_batteries(self) -> Set[int]:
        """Get subset with ids of working batteries.

        Raises:
            RuntimeError: If this method was called before `run` method.

        Returns:
            Set[int]: Set of batteries that are working and can be charged ot
                discharged.
        """
        if len(self._battery_receivers) == 0:
            raise RuntimeError(
                "User didn't subscribed for the component data. Call `run` method "
                "before the first use."
            )

        now = datetime.datetime.now(tz=pytz.UTC)

        working_batteries: Set[int] = set()
        # set of batteries that we don't know if are working.
        unknown_batteries: Set[int] = set()
        for battery_id, inverter_id in self._bat_inv_map.items():
            inv_msg = self._inverter_receivers[inverter_id].peek()
            bat_msg = self._battery_receivers[battery_id].peek()

            if inv_msg is None or bat_msg is None:
                if self._last_status[battery_id].last_message_received is True:
                    _logger.warning(
                        "No message from component %d or %d", battery_id, inverter_id
                    )
                    self._last_status[battery_id].last_message_received = False
                continue

            # If data are old then we can't assume anything
            if self._is_outdated(now, bat_msg, inv_msg):
                continue

            last_status_correct = self._last_status[battery_id].last_status_correct
            is_status_correct = self._is_status_correct(bat_msg, inv_msg)
            if is_status_correct is False:
                continue
            if is_status_correct is True and last_status_correct is False:
                # If the status changed from invalid to valid, then try to use that
                # battery independently from other conditions.
                working_batteries.add(battery_id)
                continue

            if self._is_error_code_critical(bat_msg, inv_msg):
                unknown_batteries.add(battery_id)
                continue

            if not self._last_response.is_correct(battery_id, now):
                unknown_batteries.add(battery_id)

            working_batteries.add(battery_id)

        if len(working_batteries) == 0:
            if len(unknown_batteries) == 0:
                _logger.error("All batteries considered as broken!")
            elif self._last_returned_unknown_batteries is False:
                _logger.error(
                    "All batteries considered as broken, start using batteries in unknown state."
                )
                self._last_returned_unknown_batteries = True
            return unknown_batteries

        self._last_returned_unknown_batteries = False
        return working_batteries

    def _get_component_pairs(self, battery_pool: Set[int]) -> Dict[int, int]:
        """Create map with batteries in the pool and inverters adjacent to them.

        For each battery in the pool, find adjacent inverter.

        Args:
            battery_pool: pool of batteries that should be used.

        Returns:
            Map between battery id and adjacent inverter id.
        """
        component_graph = microgrid_api.get().component_graph
        bat_inv_map: Dict[int, int] = {}

        for battery_id in battery_pool:
            predecessors = component_graph.predecessors(battery_id)
            for component in predecessors:
                if component.category == ComponentCategory.INVERTER:
                    bat_inv_map[battery_id] = component.component_id

            if battery_id not in bat_inv_map:
                _logger.error("Battery %d has no inverter", battery_id)

        return bat_inv_map

    def _is_outdated(
        self, now: datetime.datetime, bat_msg: BatteryData, inv_msg: InverterData
    ) -> bool:
        """Return if any message is to old to be considered as valid data.

        Args:
            now: current timestamp
            bat_msg: battery message
            inv_msg: inverter message

        Returns:
            True if message is outdated, False otherwise.
        """
        older_msg_timestamp = min(bat_msg.timestamp, inv_msg.timestamp)
        outdated = (now - older_msg_timestamp).seconds > self._max_inactive_duration

        bat_id = bat_msg.component_id
        if outdated != self._last_status[bat_id].last_message_not_outdated:
            if outdated is True:
                _logger.info(
                    "Battery %d or inverter %d stopped sending since %f sec. Stopped using it.",
                    bat_msg.component_id,
                    inv_msg.component_id,
                    self._max_inactive_duration,
                )
            else:
                _logger.info(
                    "Battery %d or inverter %d started sending data.",
                    bat_msg.component_id,
                    inv_msg.component_id,
                )
            self._last_status[bat_id].last_message_not_outdated = outdated
        return outdated

    def _is_status_correct(self, bat_msg: BatteryData, inv_msg: InverterData) -> bool:
        """Return if status of the both components is valid.

        Args:
            bat_msg: battery message
            inv_msg: inverter message

        Returns:
            True if status of the components is valid, False otherwise..
        """
        battery_id = bat_msg.component_id
        if (
            inv_msg.component_state in BatteryStatus.inverter_invalid_state
            or bat_msg.component_state in BatteryStatus.battery_invalid_state
            or bat_msg.relay_state in BatteryStatus.battery_invalid_relay
        ):
            if self._last_status[battery_id].last_status_correct is True:
                _logger.info(
                    "Battery %d has invalid state, stopped using it.",
                    battery_id,
                )
                self._last_status[battery_id].last_status_correct = False
            return False

        if self._last_status[battery_id].last_status_correct is False:
            _logger.info(
                "Battery %d changed status, start using it.",
                battery_id,
            )
            self._last_status[battery_id].last_status_correct = True

        return True

    def _is_error_code_critical(
        self, bat_msg: BatteryData, inv_msg: InverterData
    ) -> bool:
        """Check error levels of the components and return if any error is critical.

        Args:
            bat_msg: battery data
            inv_msg: inverter data

        Returns:
            bool: True if any error level in any message is critical.
        """
        critical = common_pb.ErrorLevel.ERROR_LEVEL_CRITICAL

        is_critical = any(error.level == critical for error in bat_msg.errors) or any(
            error.level == critical for error in inv_msg.errors
        )
        bat_id = bat_msg.component_id
        if is_critical != self._last_status[bat_id].last_error_code_not_critical:
            bat_id = bat_msg.component_id
            if is_critical is True:
                _logger.info("Battery %d change error code to critical.", bat_id)
            else:
                _logger.info("Battery %d has no more critical error code.", bat_id)
            self._last_status[bat_id].last_error_code_not_critical = is_critical

        return is_critical
