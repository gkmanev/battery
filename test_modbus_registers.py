"""Protocol regression tests; no production DB, Blynk, or remote connections."""

import asyncio
import importlib
import socket
import struct
import sys
import types
import unittest
from unittest.mock import Mock, patch

from pymodbus.client import AsyncModbusTcpClient
from pymodbus.server import StartAsyncTcpServer, ServerAsyncStop

import modbus_registers as registers

# The server module's database dependency initializes a SQLite file on import.
# Replace it while loading the module so tests never touch the production DB.
database_stub = types.ModuleType("database")
database_stub.BatteryActualState = Mock()
database_stub.BatterySchedule = Mock()
database_stub.SessionLocal = Mock()
with patch.dict(sys.modules, {"database": database_stub}):
    scada_module = importlib.import_module("battery_scada")
BatteryScada = scada_module.BatteryScada


def decode(words):
    return struct.unpack(">f", struct.pack(">HH", *words))[0]


class RegisterTests(unittest.TestCase):
    def setUp(self):
        # Exercise the real constructor and datastore, without starting workers.
        with patch.object(BatteryScada, "get_current_state_of_charge"), patch.object(
            scada_module.threading, "Thread"
        ):
            self.battery = BatteryScada("test", blynk_token="")
        self.store = self.battery.context[0]

    def read(self, address, count):
        return self.store.getValues(3, address, count)

    def test_full_initial_map_and_unscaled_ratings(self):
        self.assertEqual(self.read(0, 14), [0] * 14)
        self.assertEqual(self.read(14, 6), [0] * 6)
        self.assertEqual(self.read(20, 4), [80, 25, 1000, 1000])
        self.assertFalse(self.store.validate(3, 4, 2))
        self.assertFalse(self.store.validate(3, 24, 10))
        self.assertFalse(self.store.validate(3, 0, 24))
        self.assertTrue(self.store.validate(3, 0, 4))
        self.assertTrue(self.store.validate(3, 6, 18))
        self.assertIsNone(self.battery.p_setpoint_kw)
        self.assertEqual(registers.BESS_UPDATE_INTERVAL_S, 0.2)
        self.assertEqual(self.battery.simulation_interval_s, 0.2)

    def test_float_status_and_neighboring_registers(self):
        self.battery.state_of_charge = 62.5
        self.battery.actual_invertor_power = -250.0
        self.battery.publish_modbus_status()
        words = self.read(14, 6)
        self.assertEqual(words[:2], [0x427A, 0x0000])
        self.assertEqual(decode(words[:2]), 62.5)
        self.assertEqual(decode(words[2:4]), -0.25)
        self.assertEqual(decode(words[4:6]), 0.0)
        self.assertEqual(self.read(20, 4), [80, 25, 1000, 1000])
        self.assertIsNone(self.battery.p_setpoint_kw)

    def test_commands_are_independent_at_exact_wire_addresses(self):
        self.store.setValues(16, 0, BatteryScada._float_to_regs(0.5))
        self.store.setValues(16, 2, BatteryScada._float_to_regs(-0.25))
        self.assertEqual(self.battery.p_setpoint_kw, 500.0)
        self.assertEqual(self.battery.wt_setpoint_mw, -0.25)
        self.assertEqual(self.read(20, 4), [80, 25, 1000, 1000])

    def test_batched_commands_decode_each_pair(self):
        values = sum((BatteryScada._float_to_regs(p) for p in (-0.5, 0.25)), [])
        self.store.setValues(16, 0, values)
        self.assertEqual(self.battery.p_setpoint_kw, -500.0)
        self.assertEqual(self.battery.wt_setpoint_mw, 0.25)
        self.store.setValues(16, 0, BatteryScada._float_to_regs(0.0))
        self.assertEqual(self.battery.p_setpoint_kw, 0.0)


class WireTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_tcp_read_and_write(self):
        with patch.object(BatteryScada, "get_current_state_of_charge"), patch.object(
            scada_module.threading, "Thread"
        ):
            battery = BatteryScada("test", blynk_token="")
        battery.state_of_charge = 75.0
        battery.actual_invertor_power = 500.0
        battery.publish_modbus_status()
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        task = asyncio.create_task(StartAsyncTcpServer(
            context=battery.context, address=("127.0.0.1", port)
        ))
        client = AsyncModbusTcpClient("127.0.0.1", port=port)
        try:
            for _ in range(40):
                await asyncio.sleep(0.025)
                if await client.connect():
                    break
            self.assertTrue(client.connected)
            response = await client.read_holding_registers(address=14, count=10)
            self.assertFalse(response.isError())
            self.assertEqual(decode(response.registers[:2]), 75.0)
            self.assertEqual(decode(response.registers[2:4]), 0.5)
            self.assertEqual(response.registers[6:10], [80, 25, 1000, 1000])
            for address, count in ((4, 2), (24, 10), (0, 24)):
                response = await client.read_holding_registers(address=address, count=count)
                self.assertTrue(response.isError())
                self.assertEqual(response.exception_code, 2)
            for address in (4, 24):
                response = await client.write_registers(
                    address=address, values=BatteryScada._float_to_regs(1.0)
                )
                self.assertTrue(response.isError())
                self.assertEqual(response.exception_code, 2)
            self.assertIsNone(battery.p_setpoint_kw)
            response = await client.write_registers(
                address=0, values=BatteryScada._float_to_regs(-0.5)
            )
            self.assertFalse(response.isError())
            self.assertEqual(battery.p_setpoint_kw, -500.0)
            response = await client.write_registers(
                address=2, values=BatteryScada._float_to_regs(0.25)
            )
            self.assertFalse(response.isError())
            self.assertEqual(battery.p_setpoint_kw, -500.0)
            self.assertEqual(battery.wt_setpoint_mw, 0.25)
        finally:
            client.close()
            await ServerAsyncStop()
            await asyncio.wait_for(task, timeout=2)


if __name__ == "__main__":
    unittest.main()
