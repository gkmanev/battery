
import asyncio
import json
import logging
import os
import sys
import threading
import time
import traceback
from datetime import datetime, timedelta
from typing import Optional
from dotenv import load_dotenv
import pandas as pd
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
#from PIL import Image, ImageDraw, ImageFont
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
#from waveshare_epd import epd2in7_V2

from database import BatteryActualState, BatterySchedule, SessionLocal
from mqtt_client import MqttClient
load_dotenv()

logging.basicConfig(level=logging.DEBUG)


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

        self.modbus_thread = None
        self.mqtt_client = mqtt_client
        self.schedule_url = schedule_url or os.getenv(
            "BATTERY_SCHEDULE_URL", "http://85.14.6.37:16543/api/schedule/"
        )
        self.blynk_token = blynk_token or os.getenv("BLYNK_TOKEN")
        self.mqtt_topic = mqtt_topic or os.getenv(
            "BATTERY_MQTT_TOPIC", f"battery_scada/{self.batt_id}"
        )
        self._last_displayed = None
        self.modbus_retry_delay = 5
        self.get_current_state_of_charge()
        self.init_modbus_server()

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

    def init_modbus_server(self):
        try:
            zeros = [0] * 100

            def on_hr_write(address, values):
                # HR10 is our active power setpoint (kW * 10, signed)
                # In pymodbus 3.x, the address passed here is 1-based, so HR10 comes as address=11
                SETPOINT_REG = 11  # Changed from 10 to 11 for pymodbus 3.x
                logging.debug(f"on_hr_write called: address={address}, values={values}")
                
                # Check if SETPOINT_REG is within the written range
                if address <= SETPOINT_REG < address + len(values):
                    idx = SETPOINT_REG - address
                    raw = values[idx]

                    # Interpret as signed 16-bit
                    if raw >= 0x8000:
                        raw = raw - 0x10000

                    p_kw = raw / 10.0
                    self.set_active_power_setpoint(p_kw)
                    logging.info(f"Modbus HR10 (internal addr {SETPOINT_REG}) written: raw={raw}, p_kw={p_kw}")
                else:
                    logging.debug(f"Write to address {address} does not affect setpoint register {SETPOINT_REG}")

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
        quarter_min = self.lookup_quarterly(timenow.minute)
        quarter_hour = timenow.hour + 1 if quarter_min == 0 else timenow.hour
        target_timestamp = timenow.replace(hour=quarter_hour, minute=quarter_min, second=0, microsecond=0)
        print(f"Target Timestamp: {target_timestamp}")
        try:
            with SessionLocal() as session:
                result = session.query(BatterySchedule).filter(
                    BatterySchedule.timestamp == target_timestamp
                ).first()
                if result:
                    print(f"Schedule for {target_timestamp} is {result.schedule}")
                    return float(result.schedule)
                else:
                    print("No matching schedule found.")
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

    def update_actual_battery_state_in_db(self):
        """Compute one-minute step of battery state and honor Modbus overrides.

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
          written to the status table with the minute-resolution timestamp.
        """

        baseline_kw = self.actual_battery_state()  # from DB schedule (kW)
        # If Modbus setpoint has been written, use it. Otherwise use schedule.
        modbus_override = self.p_setpoint_kw is not None
        if modbus_override:
            requested_power_kw = self.p_setpoint_kw
        else:
            requested_power_kw = baseline_kw

        logging.info(
            f"Baseline (schedule): {baseline_kw:.1f} kW | "
            f"Requested P (after Modbus): {requested_power_kw:.1f} kW | "
            f"SoC: {self.state_of_charge:.2f}%"
        )
        
        # Limit power to ± rated power
        requested_power_kw = max(-self.bess_power_kw, min(self.bess_power_kw, requested_power_kw))
        # Enforce SoC band (AS requirement) ----
        # If a Modbus setpoint is pushing us beyond SoC bounds, fall back to the baseline schedule.
        while True:
            limit_hit = False
            if self.state_of_charge <= self.soc_min_percent and requested_power_kw < 0:
                if modbus_override:
                    logging.info(
                        "Discharge blocked by SoC minimum; reverting to baseline schedule."
                    )
                    self.p_setpoint_kw = None
                    requested_power_kw = baseline_kw
                    modbus_override = False
                    limit_hit = True
                else:
                    print("Blocking discharge: SoC at/under minimum for AS support.")
                    requested_power_kw = 0.0
            if self.state_of_charge >= self.soc_max_percent and requested_power_kw > 0:
                if modbus_override:
                    logging.info(
                        "Charge blocked by SoC maximum; reverting to baseline schedule."
                    )
                    self.p_setpoint_kw = None
                    requested_power_kw = baseline_kw
                    modbus_override = False
                    limit_hit = True
                else:
                    print("Blocking charge: SoC at/over maximum for AS support.")
                    requested_power_kw = 0.0

            # After falling back to baseline, re-check limits once to ensure compliance.
            if not limit_hit:
                break

        # Compute SoC change for 1 minute step ----        
        dt_hours = 1.0 / 60.0  # 1 minute step        
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

        print(
            f"SoC: {self.state_of_charge:.2f}% || "
            f"Last Minute Energy Change: {energy_change_kwh:.4f} kWh || "
            f"Actual Inv Pow: {self.actual_invertor_power:.1f} kW"
        )

        timenow = datetime.now()
        timestamp = timenow.replace(second=0, microsecond=0)
        try:
            with SessionLocal() as session:
                actual_state_entry = BatteryActualState(
                    timestamp=timestamp,
                    battery_state_of_charge_actual=self.state_of_charge,
                    last_min_flow=self.energy_flow_minute,
                    invertor_power_actual=self.actual_invertor_power
                )
                session.add(actual_state_entry)
                session.commit()
        except Exception as e:
            logging.error(f"Error saving status to DB: {e}")


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
                        "flow_last_min": result.last_min_flow,
                        "invertor": result.invertor_power_actual
                    }                    
                    json_data = json.dumps(self.actual_data)
                    print(f"MQTT: {json_data}")
                    if self.mqtt_client:
                        self.mqtt_client.publish_message(json_data)
                    #self.display_data(max(0, min(result.battery_state_of_charge_actual, 100)), result.invertor_power_actual)
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
            except requests.exceptions.RequestException as e:
                print(f"Failed to publish {pin}: {value}, Error: {e}")

    # def display_data(self, soc, invertor):
    #     if soc is not None and invertor is not None:
    #         batt_status = "Idle"
    #         if invertor > 0:
    #             batt_status = "Charging"
    #         elif invertor < 0:
    #             batt_status = "Discharging"

    #         current_payload = (soc, invertor)
    #         if self._last_displayed == current_payload:
    #             return
    #         self._last_displayed = current_payload

    #         script_dir = os.path.dirname(os.path.realpath(__file__))
    #         picdir = os.path.join(script_dir, 'pic')
    #         libdir = os.path.join(script_dir, 'lib')
    #         if os.path.exists(libdir):
    #             sys.path.append(libdir)
    #         try:
    #             epd = epd2in7_V2.EPD()
    #             epd.init()
    #             epd.Clear()
    #             font_path = os.path.join(picdir, 'Font.ttc')
    #             try:
    #                 font24 = ImageFont.truetype(font_path, 24)
    #                 font20 = ImageFont.truetype(font_path, 18)
    #             except IOError:
    #                 font24 = ImageFont.load_default()
    #                 font20 = ImageFont.load_default()
    #             image = Image.new('1', (epd.height, epd.width), 255)
    #             draw = ImageDraw.Draw(image)
    #             current_time = time.strftime('%d-%m-%Y %H:%M')
    #             cell_width = 80
    #             cell_height = 40
    #             draw.rectangle((0, 0, epd.height, epd.width), fill=255)
    #             draw.rectangle((0, 0, cell_width, cell_height), outline=0)
    #             draw.text((8, 10), "Battery1", font=font20, fill=0)
    #             draw.rectangle((cell_width, 0, cell_width * 2+20, cell_height), outline=0)
    #             draw.text((90, 10), "100MW/h", font=font20, fill=0)
    #             draw.rectangle((cell_width, 0, cell_width * 3 +20, cell_height), outline=0)
    #             draw.text((190, 10), "25MW", font=font20, fill=0)
    #             draw.text((8, 45), current_time, font=font20, fill=0)
    #             draw.text((8, 90), f"SoC: {soc} MW/h", font=font20, fill=0)
    #             draw.text((8, 120), f"{batt_status}: {invertor} MW", font=font20, fill=0)
    #             epd.display(epd.getbuffer(image))
    #         except IOError as e:
    #             logging.info(e)
    #             logging.error(traceback.format_exc())
    #         except KeyboardInterrupt:
    #             logging.info("ctrl + c:")
    #             epd2in7_V2.epdconfig.module_exit(cleanup=True)
    #             exit()

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
    scheduler.add_job(test.update_actual_battery_state_in_db, CronTrigger(minute='*'))
    scheduler.add_job(test.fetch_actual_db, CronTrigger(minute='*'))
    scheduler.start()

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        mqtt_client.disconnect_client()
