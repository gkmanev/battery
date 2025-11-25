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

import pandas as pd
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from PIL import Image, ImageDraw, ImageFont
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
from waveshare_epd import epd2in7_V2

from database import BatteryActualState, BatterySchedule, SessionLocal
from mqtt_client import MqttClient

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
    ) -> None:
        self.state_of_charge = 0
        self.battery_state = "Idle"
        self.excel_workbook = None
        self.actual_invertor_power = 0
        self.round_trip = round_trip
        self.actual_data = {}
        self.batt_id = batt_id
        self.modbus_thread = None
        self.mqtt_client = mqtt_client
        self.schedule_url = schedule_url or os.environ.get(
            "BATTERY_SCHEDULE_URL", "http://85.14.6.37:16543/api/schedule/"
        )
        self.blynk_token = blynk_token or os.environ.get("BLYNK_TOKEN")
        self.mqtt_topic = mqtt_topic or os.environ.get(
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

            if HAVE_SLAVE_CTX:
                store = ModbusSlaveContext(
                    di=ModbusSequentialDataBlock(0, zeros.copy()),
                    co=ModbusSequentialDataBlock(0, zeros.copy()),
                    hr=ModbusSequentialDataBlock(0, zeros.copy()),
                    ir=ModbusSequentialDataBlock(0, zeros.copy()),
                    zero_mode=True,
                )
            else:
                store = _DeviceContext(
                    di=ModbusSequentialDataBlock(0, zeros.copy()),
                    co=ModbusSequentialDataBlock(0, zeros.copy()),
                    hr=ModbusSequentialDataBlock(0, zeros.copy()),
                    ir=ModbusSequentialDataBlock(0, zeros.copy()),
                )

            context = ModbusServerContext(store, True)
            self.context = context

            self.modbus_thread = threading.Thread(
                target=self.start_modbus_thread, daemon=True
            )
            self.modbus_thread.start()
            logging.info("Modbus server started in a separate thread.")
        except Exception as e:
            logging.error(f"Error initializing Modbus server: {e}")
            logging.error(traceback.format_exc())

    def get_current_state_of_charge(self):
        try:
            with SessionLocal() as session:
                result = session.query(BatteryActualState).order_by(BatteryActualState.timestamp.desc()).first()
                if result:
                    print(result)
                    self.state_of_charge = result.battery_state_of_charge_actual
                    print(self.state_of_charge)
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
            logging.error(f"Error saving status to DB: {e}")

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
                    return result.schedule
                else:
                    print("No matching schedule found.")
                    return None
        except Exception as e:
            print(f"Error fetching schedule: {e}")
            return None

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
        current_status = self.actual_battery_state()
        if current_status is not None:
            energy_flow_minute = (current_status / 60) * self.round_trip
            self.state_of_charge += energy_flow_minute
            self.state_of_charge = max(0, min(self.state_of_charge, 100))
            self.energy_flow_minute = current_status / 60
            self.actual_invertor_power = current_status
            print(
                f"soc:{round(self.state_of_charge, 2):.2f} || Last Minute Flow: {self.energy_flow_minute} || Actual Inv Pow: {self.actual_invertor_power}"
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
                    soc_safe = max(0, min(65535, int(result.battery_state_of_charge_actual))) if result.battery_state_of_charge_actual is not None else 0
                    try:
                        self.context[0x00].setValues(3, 0, [soc_safe, 1])
                    except Exception as e:
                        logging.error(f"Error updating Modbus register: {e}")
                    self.actual_data = {
                        "devId": self.batt_id,
                        "timestamp": result.timestamp.strftime('%Y-%m-%d %H:%M'),
                        "soc": max(0, min(result.battery_state_of_charge_actual, 100)),
                        "flow_last_min": result.last_min_flow,
                        "invertor": result.invertor_power_actual
                    }
                    print(self.actual_data)
                    json_data = json.dumps(self.actual_data)
                    if self.mqtt_client:
                        self.mqtt_client.publish_message(json_data)
                    self.display_data(max(0, min(result.battery_state_of_charge_actual, 100)), result.invertor_power_actual)
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
                print(f"Published {pin}: {value}, Status Code: {response.status_code}")
            except requests.exceptions.RequestException as e:
                print(f"Failed to publish {pin}: {value}, Error: {e}")

    def display_data(self, soc, invertor):
        if soc is not None and invertor is not None:
            batt_status = "Idle"
            if invertor > 0:
                batt_status = "Charging"
            elif invertor < 0:
                batt_status = "Discharging"

            current_payload = (soc, invertor)
            if self._last_displayed == current_payload:
                return
            self._last_displayed = current_payload

            script_dir = os.path.dirname(os.path.realpath(__file__))
            picdir = os.path.join(script_dir, 'pic')
            libdir = os.path.join(script_dir, 'lib')
            if os.path.exists(libdir):
                sys.path.append(libdir)
            try:
                epd = epd2in7_V2.EPD()
                epd.init()
                epd.Clear()
                font_path = os.path.join(picdir, 'Font.ttc')
                try:
                    font24 = ImageFont.truetype(font_path, 24)
                    font20 = ImageFont.truetype(font_path, 18)
                except IOError:
                    font24 = ImageFont.load_default()
                    font20 = ImageFont.load_default()
                image = Image.new('1', (epd.height, epd.width), 255)
                draw = ImageDraw.Draw(image)
                current_time = time.strftime('%d-%m-%Y %H:%M')
                cell_width = 80
                cell_height = 40
                draw.rectangle((0, 0, epd.height, epd.width), fill=255)
                draw.rectangle((0, 0, cell_width, cell_height), outline=0)
                draw.text((8, 10), "Battery1", font=font20, fill=0)
                draw.rectangle((cell_width, 0, cell_width * 2+20, cell_height), outline=0)
                draw.text((90, 10), "100MW/h", font=font20, fill=0)
                draw.rectangle((cell_width, 0, cell_width * 3 +20, cell_height), outline=0)
                draw.text((190, 10), "25MW", font=font20, fill=0)
                draw.text((8, 45), current_time, font=font20, fill=0)
                draw.text((8, 90), f"SoC: {soc} MW/h", font=font20, fill=0)
                draw.text((8, 120), f"{batt_status}: {invertor} MW", font=font20, fill=0)
                epd.display(epd.getbuffer(image))
            except IOError as e:
                logging.info(e)
                logging.error(traceback.format_exc())
            except KeyboardInterrupt:
                logging.info("ctrl + c:")
                epd2in7_V2.epdconfig.module_exit(cleanup=True)
                exit()

    def empty_table(self):
        try:
            with SessionLocal() as session:
                session.query(BatterySchedule).delete()
                session.commit()
                print("Table emptied successfully.")
        except Exception as e:
            print(f"Error emptying table: {e}")


if __name__ == "__main__":
    mqtt_client = MqttClient("159.89.103.242", 1883, "battery_scada/batt-0001")
    mqtt_client.connect_client()
    test = BatteryScada(batt_id="batt-0001", round_trip=0.97, mqtt_client=mqtt_client)
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
