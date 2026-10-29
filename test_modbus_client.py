"""Verify client encoding and reads without connecting to the remote plant."""

import unittest
from unittest.mock import Mock, patch

import modbus_client


class ClientTests(unittest.TestCase):
    def setUp(self):
        self.client = Mock()
        self.patch_client = patch.object(modbus_client, "client", self.client)
        self.patch_client.start()
        self.addCleanup(self.patch_client.stop)
        self.output = patch("builtins.print").start()
        self.addCleanup(patch.stopall)

    def response(self, words):
        self.client.read_holding_registers.return_value = Mock(
            registers=words, isError=Mock(return_value=False)
        )

    def test_battery_status_decodes_ieee_float32(self):
        self.response([0x427A, 0, 0xBE80, 0, 0, 0])
        modbus_client.read_status()
        self.client.read_holding_registers.assert_called_once_with(address=14, count=6)
        self.output.assert_called_once_with(
            "SOC_BESS_MV: 62.50% | P_BESS_MV: -0.250 MW | Q_BESS_MV: 0.000 MVAr"
        )

    def test_unscaled_ratings_order(self):
        self.response([80, 25, 1000, 1000])
        modbus_client.read_ratings()
        self.client.read_holding_registers.assert_called_once_with(address=20, count=4)
        self.output.assert_called_once_with(
            "Prated_BESS_MV: 1000.0 kW | Capacity_BESS_MV: 1000.0 kWh | "
            "SOC_MIN_BESS_MV: 25.00% | SOC_MAX_BESS_MV: 80.00%"
        )

    def test_commands_and_wt_use_separate_valid_ranges(self):
        self.response([0x3F00, 0, 0x3F80, 0])
        modbus_client.read_commands()
        self.client.read_holding_registers.assert_called_with(address=0, count=4)
        self.output.assert_called_with("DP_BESS_MV: 0.500 MW | DP_WT_MV: 1.000 MW (stored only)")
        self.response([0] * 8)
        modbus_client.read_wt_status()
        self.client.read_holding_registers.assert_called_with(address=6, count=8)
        self.output.assert_called_with(
            "WT placeholders | P_WT: 0.000 MW | Q_WT: 0.000 MVAr | "
            "V_WT: 0.000 kV | F_WT: 0.000 Hz"
        )

    def test_setpoints_write_both_words_and_check_response(self):
        self.client.write_registers.return_value.isError.return_value = False
        self.assertTrue(modbus_client.write_dp_bess_mv(-0.5))
        self.client.write_registers.assert_called_with(address=0, values=[0xBF00, 0])
        self.assertTrue(modbus_client.write_dp_wt_mv(0.5))
        self.client.write_registers.assert_called_with(address=2, values=[0x3F00, 0])
        self.client.write_registers.return_value.isError.return_value = True
        self.output.reset_mock()
        self.assertFalse(modbus_client.write_dp_bess_mv(0.5))
        self.assertIn("Error writing", self.output.call_args.args[0])

    def test_incomplete_and_error_responses_do_not_report_measurements(self):
        self.response([0])
        modbus_client.read_status()
        self.output.assert_called_once_with("Incomplete response for HR14-19")
        self.client.read_holding_registers.return_value.isError.return_value = True
        self.output.reset_mock()
        modbus_client.read_status()
        self.assertIn("Error reading HR14-19", self.output.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
