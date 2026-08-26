
import asyncio
import json
import logging
import os
import threading
import time
import traceback
import struct
from datetime import datetime, timedelta
from typing import Optional
from dotenv import load_dotenv
import pandas as pd
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from pymodbus.datastore import ModbusSequentialDataBlock, ModbusServerContext
try:
    # pymodbus 2.x
    from pymodbus.datastore import ModbusSlaveContext
    HAVE_SLAVE_CTX = True
except ImportError:
    # pymodbus 3.x+
    from pymodbus.datastore import ModbusDeviceContext as _DeviceContext
    HAVE_SLAVE_CTX = False
from pymodbus.server import StartAsyncTcpServer

from database import BatteryActualState, BatterySchedule, SessionLocal
from mqtt_client import MqttClient
load_dotenv()

logging.basicConfig(level=logging.INFO)

# Reduce noise from third-party loggers.
for _logger_name in ("apscheduler", "urllib3", "asyncio", "tzlocal"):
    logging.getLogger(_logger_name).setLevel(logging.WARNING)
logging.getLogger("pymodbus").setLevel(logging.WARNING)


class _PymodbusSetValuesFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        if "setValues" not in msg:
            return False

        # Show status writes (SoC + power), setpoint writes, and startup ratings.
        if "address-15: count-4" in msg or "address-14: count-4" in msg:
            return True
        if "address-1: count-2" in msg:
            return True
        if "address-3: count-4" in msg:
            return True

        return False


_pymodbus_logger = logging.getLogger("pymodbus.logging")
_pymodbus_logger.setLevel(logging.DEBUG)
_pymodbus_logger.propagate = False
_pymodbus_handler = logging.StreamHandler()
_pymodbus_handler.setLevel(logging.DEBUG)
_pymodbus_handler.addFilter(_PymodbusSetValuesFilter())
_pymodbus_handler.setFormatter(logging.Formatter("%(levelname)s:%(name)s:%(message)s"))
_pymodbus_logger.addHandler(_pymodbus_handler)

# Modbus holding register map (0-based).
REG_SETPOINT_BASES = (0, 1)          # HR0/1 float32 MW setpoint
REG_RATED_POWER = 2                  # HR2 uint16 kW*10
REG_CAPACITY = 3                     # HR3 uint16 kWh*10
REG_SOC_MIN = 4                      # HR4 uint16 %*100
REG_SOC_MAX = 5                      # HR5 uint16 %*100
REG_SOC = 14                         # HR14 uint16 %*100
REG_POWER_BASE = 16                  # HR16/17 float32 MW actual power
REG_POWER_PAD = 15                   # HR15 pad


class BatteryScada:
    def __init__(
        self,
        batt_id: str,
        round_trip: float = 1,
        *,
        mqtt_client: Optional[MqttClient] = None,
        schedule_url: Optional[str] = None,
        blynk_token: Optional[str] = None,
        mqtt_topic: Optional[str] = None,
        soc_min_percent: float = 25.0,
        soc_max_percent: float = 80.0,
        bess_power_kw: float = 1000.0,  # 1 MW
        bess_capacity_kwh: float = 1000.0,  # 1 MWh
    ) -> None:
        self.state_of_charge = 0
        self.battery_state = "Idle"
        self.excel_workbook = None
        self.actual_invertor_power = 0
        self.round_trip = round_trip
        self.actual_data = {}
        self.batt_id = batt_id
        self.soc_min_percent = soc_min_percent
        self.soc_max_percent = soc_max_percent
        self.bess_power_kw = bess_power_kw
        self.bess_capacity_kwh = bess_capacity_kwh
        self.p_setpoint_kw = None  # active power setpoint from Modbus or schedule (kW)
        self._last_power_kw = None
        self._last_power_timestamp = None

        self.modbus_thread = None
        self.mqtt_client = mqtt_client
        self.schedule_url = schedule_url or os.getenv(
            "BATTERY_SCHEDULE_URL", "http://85.14.6.37:16543/api/schedule/"
        )
        self.blynk_token = blynk_token or os.getenv("BLYNK_TOKEN")
        self.mqtt_topic = mqtt_topic or os.getenv(
            "BATTERY_MQTT_TOPIC", f"battery_scada/{self.batt_id}"
        )
        self.modbus_retry_delay = 5
        self.simulation_interval_s = 1.0
        self.db_write_interval_s = 60.0
        self.publish_interval_s = 60.0
        self.log_interval_s = 1.0
        self._last_db_write = None
        self._last_publish = None
        self._last_log = None
        self._last_schedule_log = None
        self._last_schedule_value = None
        self.get_current_state_of_charge()
        self.init_modbus_server()
        self.start_simulation_thread()

    def start_modbus_thread(self):
        async def run_server():
            while True:
                try:
                    await StartAsyncTcpServer(context=self.context, address=("0.0.0.0", 5020))
                except Exception as e:
                    logging.error(f"Modbus Server encountered exception: {e}")
                    logging.error(traceback.format_exc())
                    await asyncio.sleep(self.modbus_retry_delay)
                else:
                    break

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run_server())

    def start_simulation_thread(self):
        self.simulation_thread = threading.Thread(
            target=self._simulation_loop, daemon=True
        )
        self.simulation_thread.start()

    def _simulation_loop(self):
        interval = self.simulation_interval_s
        next_tick = time.monotonic()
        while True:
            try:
                self.update_actual_battery_state_in_db(dt_seconds=interval)
            except Exception as e:
                logging.error(f"Simulation loop error: {e}")
                logging.error(traceback.format_exc())
            next_tick += interval
            sleep_for = next_tick - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)
            else:
                # If we're behind, reset the schedule to avoid drift.
                next_tick = time.monotonic()

    def init_modbus_server(self):
        try:
            zeros = [0] * 100

            def on_hr_write(address, values):
                # HR0/HR1 are our active power setpoint (MW, float32).
                # In pymodbus 3.x, the address passed here can be 1-based.
                SETPOINT_BASES = REG_SETPOINT_BASES
                logging.debug(f"on_hr_write called: address={address}, values={values}")
                
                # Need both registers (float32) to decode.
                for base in SETPOINT_BASES:
                    if address <= base and address + len(values) >= base + 2:
                        idx = base - address
                        regs = values[idx : idx + 2]
                        p_mw = self._regs_to_float(regs)
                        p_kw = p_mw * 1000.0
                        self.set_active_power_setpoint(p_kw)
                        logging.info(
                            f"Modbus setpoint (HR0/1) written: mw={p_mw:.3f}, kw={p_kw:.1f}"
                        )
                        return

            # Create the callback-enabled holding register block
            hr_block = CallbackDataBlock(0, zeros.copy(), on_write=on_hr_write)

            # pymodbus 3.x - ModbusSlaveContext without zero_mode
            store = ModbusSlaveContext(
                di=ModbusSequentialDataBlock(0, zeros.copy()),
                co=ModbusSequentialDataBlock(0, zeros.copy()),
                hr=hr_block,  # Use the callback block here
                ir=ModbusSequentialDataBlock(0, zeros.copy()),
            )

            # In pymodbus 3.x, use 'slaves' parameter
            self.context = ModbusServerContext(slaves=store, single=True)

            # Publish static ratings/limits on connect:
            # HR2 = bess_power_kw * 10
            # HR3 = bess_capacity_kwh * 10
            # HR4 = soc_min_percent * 100
            # HR5 = soc_max_percent * 100
            rated_power_scaled = int(round(self.bess_power_kw * 10))
            capacity_scaled = int(round(self.bess_capacity_kwh * 10))
            soc_min_scaled = int(round(self.soc_min_percent * 100))
            soc_max_scaled = int(round(self.soc_max_percent * 100))
            # Clamp to 16-bit unsigned range
            rated_power_scaled = max(0, min(0xFFFF, rated_power_scaled))
            capacity_scaled = max(0, min(0xFFFF, capacity_scaled))
            soc_min_scaled = max(0, min(0xFFFF, soc_min_scaled))
            soc_max_scaled = max(0, min(0xFFFF, soc_max_scaled))
            self.context[0x00].setValues(
                3,
                REG_RATED_POWER,
                [rated_power_scaled, capacity_scaled, soc_min_scaled, soc_max_scaled],
            )

            self.modbus_thread = threading.Thread(
                target=self.start_modbus_thread, daemon=True
            )
            self.modbus_thread.start()
            logging.info("Modbus server started in a separate thread.")
        except Exception as e:
            logging.error(f"Error initializing Modbus server: {e}")
            logging.error(traceback.format_exc())

    def set_active_power_setpoint(self, p_kw: float) -> None:
        """
        Receive an active power setpoint in kW.
        Positive = charge, Negative = discharge.
        """
        self.p_setpoint_kw = float(p_kw)
        logging.info(f"New active power setpoint (from Modbus): {self.p_setpoint_kw:.1f} kW")

    @staticmethod
    def _float_to_regs(value: float) -> list[int]:
        packed = struct.pack(">f", float(value))
        hi, lo = struct.unpack(">HH", packed)
        return [hi, lo]

    @staticmethod
    def _regs_to_float(regs) -> float:
        if len(regs) < 2:
            return 0.0
        hi, lo = regs[0], regs[1]
        return struct.unpack(">f", struct.pack(">HH", hi, lo))[0]

    def get_current_state_of_charge(self):
        try:
            with SessionLocal() as session:
                result = session.query(BatteryActualState).order_by(BatteryActualState.timestamp.desc()).first()
                if result:
                    print(result)
                    self.state_of_charge = result.battery_state_of_charge_actual
                    print(f"Current SoC: {self.state_of_charge}")
                else:
                    print("There are no results!")
        except Exception as e:
            print(f"Error fetching schedule: {e}")
            return None

    def fetch_schedule_endpoint(self):
        try:
            response = requests.get(self.schedule_url, timeout=10)
            data = response.json()            
            filtered_data = [entry for entry in data if entry['devId'] == self.batt_id]
            df = pd.DataFrame(filtered_data)            
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df.set_index('timestamp', inplace=True)
            df = df[['invertor']]
            df['timestamp'] = df.index
            df = df.rename(columns={'invertor': 'schedule'})
            df = df.reset_index(drop=True)
            df = df[['timestamp', 'schedule']]            
            self.save_to_db(df)
        except Exception as e:
            logging.error(f"Error occurred while fetching the endpoint: {e}")

    def save_to_db(self, df):
        try:
            with SessionLocal() as session:
                for row in df.itertuples():
                    existing_entry = session.query(BatterySchedule).filter_by(timestamp=row.timestamp).first()
                    if existing_entry:
                        existing_entry.schedule = row.schedule
                    else:
                        schedule_entry = BatterySchedule(
                            timestamp=row.timestamp,
                            battery_state="battery_state",
                            schedule=row.schedule,
                        )
                        session.add(schedule_entry)
                session.commit()
        except Exception as e:
            logging.error(f"Error saving schedule to DB: {e}")

    def actual_battery_state(self):
        timenow = datetime.now()
        def maybe_log_schedule(message, value):
            if (
                self._last_schedule_log is None
                or (timenow - self._last_schedule_log).total_seconds() >= 60
                or self._last_schedule_value is None
                or abs(value - self._last_schedule_value) >= 0.01
            ):
                print(message)
                self._last_schedule_log = timenow
                self._last_schedule_value = value
        try:
            with SessionLocal() as session:
                prev_row = (
                    session.query(BatterySchedule)
                    .filter(BatterySchedule.timestamp <= timenow)
                    .order_by(BatterySchedule.timestamp.desc())
                    .first()
                )
                next_row = (
                    session.query(BatterySchedule)
                    .filter(BatterySchedule.timestamp >= timenow)
                    .order_by(BatterySchedule.timestamp.asc())
                    .first()
                )

                if prev_row and next_row and prev_row.timestamp != next_row.timestamp:
                    total_seconds = (next_row.timestamp - prev_row.timestamp).total_seconds()
                    if total_seconds > 0:
                        elapsed_seconds = (timenow - prev_row.timestamp).total_seconds()
                        fraction = max(0.0, min(1.0, elapsed_seconds / total_seconds))
                        interpolated = prev_row.schedule + fraction * (next_row.schedule - prev_row.schedule)
                        maybe_log_schedule(
                            f"Schedule interpolated between {prev_row.timestamp} ({prev_row.schedule}) "
                            f"and {next_row.timestamp} ({next_row.schedule}) -> {interpolated:.2f}",
                            interpolated,
                        )
                        return float(interpolated)

                if prev_row:
                    maybe_log_schedule(
                        f"Schedule (hold last) for {prev_row.timestamp} is {prev_row.schedule}",
                        prev_row.schedule,
                    )
                    return float(prev_row.schedule)
                if next_row:
                    maybe_log_schedule(
                        f"Schedule (hold next) for {next_row.timestamp} is {next_row.schedule}",
                        next_row.schedule,
                    )
                    return float(next_row.schedule)

                print("No schedule entries found.")
                return 0.0
        except Exception as e:
            print(f"Error fetching schedule: {e}")
            return 0.0

    def lookup_quarterly(self, minutes):
        if 0 <= minutes <= 14:
            return 15
        elif 15 <= minutes <= 29:
            return 30
        elif 30 <= minutes <= 44:
            return 45
        elif 45 <= minutes <= 59:
            return 0
        else:
            raise ValueError("Minutes must be between 0 and 59")

    def update_actual_battery_state_in_db(self, dt_seconds: float = 60.0):
        """Compute one simulation step of battery state and honor Modbus overrides.

        Behaviour overview:
        - The baseline power command is read from the schedule table (kW).
        - A Modbus write to HR10 overrides that baseline until it is cleared.
        - The requested power is clipped to the inverter rating (±bess_power_kw).
        - If an override would violate the SoC band, the override is cleared and
          we immediately fall back to the baseline schedule for this step.
            * Discharge beyond ``soc_min_percent`` → revert to baseline.
            * Charge beyond ``soc_max_percent`` → revert to baseline.
        - After reverting we re-check the SoC limits once to ensure the
          scheduled value itself does not push outside the band; if it does,
          the command is set to 0 kW for that edge case.
        - The resulting power is applied to compute SoC/energy deltas and is
          written to the status table on a configurable interval.
        """

        baseline_kw = self.actual_battery_state()  # from DB schedule (kW)
        # If Modbus setpoint has been written, use it. Otherwise use schedule.
        modbus_override = self.p_setpoint_kw is not None
        if modbus_override:
            requested_power_kw = self.p_setpoint_kw
        else:
            requested_power_kw = baseline_kw

        log_now = datetime.now()
        if (
            self._last_log is None
            or (log_now - self._last_log).total_seconds() >= self.log_interval_s
        ):
            logging.info(
                f"Baseline (schedule): {baseline_kw:.1f} kW | "
                f"Requested P (after Modbus): {requested_power_kw:.1f} kW | "
                f"SoC: {self.state_of_charge:.2f}%"
            )
            self._last_log = log_now
        
        # Limit power to ± rated power
        requested_power_kw = max(-self.bess_power_kw, min(self.bess_power_kw, requested_power_kw))
        # Enforce SoC band (AS requirement) ----
        # Modbus setpoint always has priority over schedule; if it would
        # violate SoC limits, clamp to 0 kW (do not revert to schedule).
        if self.state_of_charge <= self.soc_min_percent and requested_power_kw < 0:
            logging.info("Discharge blocked by SoC minimum; clamping to 0 kW.")
            requested_power_kw = 0.0
        if self.state_of_charge >= self.soc_max_percent and requested_power_kw > 0:
            logging.info("Charge blocked by SoC maximum; clamping to 0 kW.")
            requested_power_kw = 0.0

        # Compute SoC change for this step ----
        dt_hours = dt_seconds / 3600.0
        energy_change_kwh = requested_power_kw * dt_hours  # kW * h = kWh

        # Apply round-trip efficiency on charging only (simple model)
        if energy_change_kwh > 0:
            energy_change_kwh *= self.round_trip

        # Convert to SoC % change: ΔSoC = (ΔE / E_cap) * 100
        delta_soc = (energy_change_kwh / self.bess_capacity_kwh) * 100.0

        # ---- 4) Update SoC and clamp to [0,100] ----
        self.state_of_charge += delta_soc
        self.state_of_charge = max(0.0, min(100.0, self.state_of_charge))

        # For logging & other variables consistent with your existing code
        self.energy_flow_minute = requested_power_kw / 60.0  # kWh/min "equivalent"
        self.actual_invertor_power = requested_power_kw
        timenow = datetime.now()
        ramp_rate_kw_per_min = None
        if self._last_power_kw is not None and self._last_power_timestamp is not None:
            time_delta_seconds = (timenow - self._last_power_timestamp).total_seconds()
            if time_delta_seconds > 0:
                ramp_rate_kw_per_min = (
                    (self.actual_invertor_power - self._last_power_kw) / time_delta_seconds * 60.0
                )
        self._last_power_kw = self.actual_invertor_power
        self._last_power_timestamp = timenow

        status_payload = {
            "soc_percent": round(self.state_of_charge, 2),
            "bess_capacity_kwh": self.bess_capacity_kwh,
            "invertor_power_kw": round(self.actual_invertor_power, 1),
            "ramp_rate_kw_per_min": None if ramp_rate_kw_per_min is None else round(ramp_rate_kw_per_min, 2),
        }

        # Update Modbus registers immediately each step.
        # HR14 = SoC (% * 100), HR16/17 = inverter power (MW, float32)
        soc_scaled = max(0, min(10000, int(self.state_of_charge * 100)))
        power_mw = self.actual_invertor_power / 1000.0
        power_regs = self._float_to_regs(power_mw)
        try:
            # Write SoC at HR14, pad HR15, then power at HR16/17.
            self.context[0x00].setValues(
                3,
                REG_SOC,
                [soc_scaled, 0, power_regs[0], power_regs[1]],
            )
        except Exception as e:
            logging.error(f"Error updating Modbus register: {e}")

        # Throttle DB writes / publishes to configured intervals.
        if (
            self._last_db_write is None
            or (timenow - self._last_db_write).total_seconds() >= self.db_write_interval_s
        ):
            timestamp = timenow.replace(second=0, microsecond=0)
            try:
                with SessionLocal() as session:
                    actual_state_entry = BatteryActualState(
                        timestamp=timestamp,
                        battery_state_of_charge_actual=self.state_of_charge,
                        last_min_flow=self.energy_flow_minute,
                        invertor_power_actual=self.actual_invertor_power,
                    )
                    session.add(actual_state_entry)
                    session.commit()
                self._last_db_write = timenow
            except Exception as e:
                logging.error(f"Error saving status to DB: {e}")

        if (
            self._last_publish is None
            or (timenow - self._last_publish).total_seconds() >= self.publish_interval_s
        ):
            print(f"\033[32m{json.dumps(status_payload)}\033[0m")
            self.actual_data = {
                "devId": self.batt_id,
                "timestamp": timenow.strftime('%Y-%m-%d %H:%M'),
                "soc": max(0, min(self.state_of_charge, 100)),
                "invertor": self.actual_invertor_power,
            }
            json_data = json.dumps(self.actual_data)
            print(f"MQTT: {json_data}")
            if self.mqtt_client:
                self.mqtt_client.publish_message(json_data)
            self.publish_to_blynk(
                max(0, min(self.state_of_charge, 100)),
                self.actual_invertor_power,
                self.energy_flow_minute,
            )
            self._last_publish = timenow


    def fetch_actual_db(self):
        timenow = datetime.now()
        timestamp_min_res = timenow.replace(second=0, microsecond=0)
        timestamp_previous_min = timestamp_min_res - timedelta(minutes=1)
        print(f"Requested Timestamp Previous Min: {timestamp_previous_min}")
        try:
            with SessionLocal() as session:
                result = session.query(BatteryActualState).filter(
                    BatteryActualState.timestamp == timestamp_previous_min
                ).first()
                if result:
                    soc_scaled = max(0, min(10000, int(result.battery_state_of_charge_actual * 100)))  # 0-10000
                    power_scaled = int(result.invertor_power_actual * 10)  # kW * 10

                    try:
                        self.context[0x00].setValues(3, 0, [soc_scaled, power_scaled])
                    except Exception as e:
                        logging.error(f"Error updating Modbus register: {e}")
                    self.actual_data = {
                        "devId": self.batt_id,
                        "timestamp": result.timestamp.strftime('%Y-%m-%d %H:%M'),
                        "soc": max(0, min(result.battery_state_of_charge_actual, 100)),
                        "invertor": result.invertor_power_actual
                    }                    
                    json_data = json.dumps(self.actual_data)
                    print(f"MQTT: {json_data}")
                    if self.mqtt_client:
                        self.mqtt_client.publish_message(json_data)
                    self.publish_to_blynk(
                        max(0, min(result.battery_state_of_charge_actual, 100)),
                        result.invertor_power_actual,
                        result.last_min_flow
                    )
                else:
                    print("There are no results!")
        except Exception as e:
            print(f"Error fetching schedule: {e}")
            return None

    def publish_to_blynk(self, soc, invertor, flow_one_min):
        if not self.blynk_token:
            logging.warning("BLYNK token not configured; skipping publish.")
            return

        data = {
            "v0": soc,
            "v1": invertor,
            "v2": flow_one_min
        }
        for pin, value in data.items():
            url = f"https://fra1.blynk.cloud/external/api/batch/update?token={self.blynk_token}&{pin}={value}"
            try:
                response = requests.get(url, timeout=5)
                response.raise_for_status()
                #print(f"Published {pin}: {value}, Status Code: {response.status_code}")
            except requests.exceptions.RequestException:
                logging.error("Failed to publish %s to Blynk.", pin)

    def empty_table(self):
        try:
            with SessionLocal() as session:
                session.query(BatterySchedule).delete()
                session.commit()
                print("Table emptied successfully.")
        except Exception as e:
            print(f"Error emptying table: {e}")


class CallbackDataBlock(ModbusSequentialDataBlock):
    """
    DataBlock that calls a callback whenever values are written.
    Used for getting active power setpoint from Modbus HR.
    """
    def __init__(self, address, values, on_write=None):
        super().__init__(address, values)
        self.on_write = on_write

    def setValues(self, address, values):
        # Let the parent store the values
        super().setValues(address, values)

        # Call callback (if any)
        if self.on_write:
            try:
                self.on_write(address, values)
            except Exception as e:
                logging.error(f"Error in Modbus write callback: {e}")



if __name__ == "__main__":

    mqtt_client = MqttClient("159.89.103.242", 1883, "battery_scada/batt-0001")
    mqtt_client.connect_client()
    test = BatteryScada(batt_id="batt1", round_trip=0.97, mqtt_client=mqtt_client)
    scheduler = BackgroundScheduler()    
    scheduler.add_job(test.fetch_schedule_endpoint, CronTrigger(minute='*'))
    scheduler.start()

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        mqtt_client.disconnect_client()
