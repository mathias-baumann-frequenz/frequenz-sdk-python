"""
Benchmarks for battery status.

Copyright
Copyright © 2022 Frequenz Energy-as-a-Service GmbH

License
MIT
"""
import asyncio
import logging

from frequenz.sdk.battery_status import BatteryStatus
from frequenz.sdk.microgrid import ComponentCategory, microgrid_api

_logger = logging.getLogger(__name__)

HOST = "157.90.243.180"
PORT = 61060


async def run() -> None:
    """Run the method."""
    await microgrid_api.initialize(HOST, PORT)
    api = microgrid_api.get()
    graph = api.component_graph
    batteries = graph.components(component_category={ComponentCategory.BATTERY})
    batteries_ids = {bat.component_id for bat in batteries}

    _logger.info("All batteries")
    _logger.info(len(batteries))
    _logger.info(batteries_ids)

    battery_status = BatteryStatus(
        battery_pool=batteries_ids, max_inactive_duration_s=5
    )

    await battery_status.run()

    while True:
        working = battery_status.get_working_batteries()
        _logger.info(len(working))
        _logger.info(working)
        await asyncio.sleep(2)


async def main() -> None:
    """Run the run() function."""
    logging.basicConfig(level=logging.DEBUG)
    await run()


asyncio.run(main())
