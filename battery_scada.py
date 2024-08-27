import pandas as pd
# import mqtt_client
import json
import datetime
import time
import sys
import os
import xlrd
import traceback
import time
import logging
from mqtt_client import MqttClient
from database import BatterySchedule, BatteryActualState, SessionLocal
from sqlalchemy import Column, Integer, String, Float, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta,date
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from PIL import Image, ImageDraw, ImageFont
from waveshare_epd import epd2in7_V2


logging.basicConfig(level=logging.DEBUG)

class BatteryScada():
    def __init__(self, batt_id, round_trip=1) -> None:
        
        self.state_of_charge = 0        
        self.battery_state = "Idle"        
        self.excel_workbook = None    
        self.actual_invertor_power = 0
        self.round_trip = round_trip
        self.actual_data = {}
        self.batt_id = batt_id
        self.get_current_state_of_charge()

    
    def get_current_state_of_charge(self):
        print("HERE!!!!!!!!!!!!!!!!!!!")
        # get current SoC when power ON the pi
        session = SessionLocal()
        try:
            result = session.query(BatteryActualState)\
                        .order_by(BatteryActualState.timestamp.desc())\
                        .first()
            if result:
                print(result)
                self.state_of_charge = result.battery_state_of_charge_actual  
                print(self.state_of_charge)             
            else:
                print(f"There are no results!")
        except Exception as e:
            print(f"Error fetching schedule: {e}")
            return None
        finally:
            session.close()  
    


    def get_file_name(self, file):
        # tomorrow = date.today()
        # d1 = tomorrow.strftime("%d.%m.%Y")
        # st = folder.split("_")[1].split("xls")[0]
        # file_date = st.split('.')
        # d = file_date[0]
        # m = file_date[1]
        # y = file_date[2]
        # name_date = d + "." + m + "." + y
        # print(f"Name Date: {name_date} || {d1}")
        #return name_date == d1 
        file_name = file.split("_")[0]
        return file_name == "ZUSE"
        
    

    def prepare_xls(self):
        try:          
            fn = "schedules"
            for root, dirs, files in os.walk(fn):                
                xlsfiles = [f for f in files if f.endswith('.xls')]
                for xlsfile in xlsfiles:
                    my_file = self.get_file_name(xlsfile)                   
                    if my_file:
                        
                        filepath = os.path.join(fn, xlsfile)                        
                        excel_workbook = xlrd.open_workbook(filepath)
                        excel_worksheet = excel_workbook.sheet_by_index(0)  
                        #Day ahead!!!
                        xl_date = date.today() + timedelta(days=1)
                        xl_date_time = str(xl_date) + "T01:15:00"
                        period = (24 * 4) 
                        schedule_list = []
                        i = 0
                        timeIndex = pd.date_range(start=xl_date_time, periods=period, freq="0h15min")
                        while i < period:
                            i += 1
                            xl_schedule = excel_worksheet.cell_value(10, 2 + i)  
                            schedule_list.append(xl_schedule)
                        df = pd.DataFrame(schedule_list, index=timeIndex)
                        df.columns = ['schedule']
                        self.save_to_db(df)

        except Exception as e:
            logging.error(f"Error occurred while preparing the Excel file: {e}") 

    def save_to_db(self, df):
        # Get the database session
        session = SessionLocal()
        try:
            for row in df.itertuples():
                
                schedule_entry = BatterySchedule(
                    timestamp=row.Index,
                    battery_state="battery_state",
                    schedule=row.schedule,               
                )

                # Add the entry to the session
                session.add(schedule_entry)
                session.commit()  # Commit the transaction
        except Exception as e:
            session.rollback()  # Rollback in case of an error
            print(f"Error saving status to DB: {e}")
        finally:
            session.close()  # Close the session   

    
    def actual_battery_state(self):
        timenow = datetime.now()
        quarter_min = self.lookup_quarterly(timenow.minute)                           
        if quarter_min == 0:
            quarter_hour = timenow.hour + 1
        else:
            quarter_hour = timenow.hour   
        
        target_timestamp = timenow.replace(hour=quarter_hour, minute=quarter_min, second=0, microsecond=0)
        print(f"Target Timestamp: {target_timestamp}")
        session = SessionLocal()         
        try:
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
        finally:
            session.close()             
        

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
            print(self.state_of_charge)         
            self.state_of_charge += self.actual_invertor_power/60            
            self.energy_flow_minute = self.actual_invertor_power/60
            self.actual_invertor_power = current_status*self.round_trip
            print(f"soc:{round(self.state_of_charge, 2):.2f} || Last Minute Flow: {self.energy_flow_minute} || Actual Inv Pow: {self.actual_invertor_power}")
            timenow = datetime.now()
            timestamp = timenow.replace(second=0, microsecond=0)
            session = SessionLocal()
            try:
                actual_state_entry = BatteryActualState(
                        timestamp = timestamp,
                        battery_state_of_charge_actual = self.state_of_charge,
                        last_min_flow = self.energy_flow_minute,
                        invertor_power_actual = self.actual_invertor_power                
                    )
                session.add(actual_state_entry)
                session.commit()  # Commit the transaction
            except Exception as e:
                session.rollback()  # Rollback in case of an error
                print(f"Error saving status to DB: {e}")
            finally:
                session.close()  # Close the session


    def fetch_actual_db(self):
        timenow = datetime.now()
        timestamp_min_res = timenow.replace(second=0, microsecond=0)
        timestamp_previous_min = timestamp_min_res - timedelta(minutes=1)
        print(f"Requested Timestamp Previous Min: {timestamp_previous_min}")
        session = SessionLocal()       
        try:
            result = session.query(BatteryActualState).filter(
                    BatteryActualState.timestamp == timestamp_previous_min
                ).first()
            if result:
                self.actual_data = {
                    "devId": self.batt_id,
                    "timestamp": result.timestamp.strftime('%Y-%m-%d %H:%M'),
                    "soc": result.battery_state_of_charge_actual,
                    "flow_last_min": result.last_min_flow,
                    "invertor": result.invertor_power_actual
                }
                print(self.actual_data)                
                
                json_data = json.dumps(self.actual_data)
                mqtt_client.publish_message(json_data)
                self.display_data(soc, invertor)

                
            else:
                print(f"There are no results!")
        except Exception as e:
            print(f"Error fetching schedule: {e}")
            return None
        finally:
            session.close()  
        
        
    def display_data(self, soc, invertor):       
        
        if soc is not None and invertor is not None:
            batt_status = "Idle"
            if invertor > 0:
                batt_status = "Charging"
            elif invertor < 0:
                batt_status = "Discharging"
            else:
                batt_status = "Idle"
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
                except IOError as e:                
                    font24 = ImageFont.load_default()  # Use default font if the specified font is not found
                    font20 = ImageFont.load_default()
                # Prepare the image with horizontal orientation    
                image = Image.new('1', (epd.height, epd.width), 255)  # 255: clear the frame
                draw = ImageDraw.Draw(image)
                
                current_time = time.strftime('%d-%m-%Y %H:%M')
                cell_width = 80
                cell_height = 40
                # Clear the entire image
                draw.rectangle((0, 0, epd.height, epd.width), fill=255)
                
                draw.rectangle((0, 0, cell_width, cell_height), outline=0)
                # Draw the current time at the top left corner
                draw.text((8, 10), "Battery1", font=font20, fill=0)
                
                draw.rectangle((cell_width, 0, cell_width * 2+20, cell_height), outline=0)
                # Draw "100MW" next to the time with adjusted spacing
                draw.text((90, 10), "100MW/h", font=font20, fill=0)
                
                draw.rectangle((cell_width, 0, cell_width * 3 +20, cell_height), outline=0)


                # Draw "25MW" next to "100MW" with adjusted spacing
                draw.text((190, 10), "25MW", font=font20, fill=0)
                
                draw.text((8, 45), current_time, font=font20, fill=0)
                
                draw.text((8, 90), f"SoC: {soc} MW/h", font=font20, fill=0)
                draw.text((8, 120), f"{batt_status}: {invertor} MW", font=font20, fill=0)

                # Perform a full update            
                epd.display(epd.getbuffer(image))
            except IOError as e:
                logging.info(e)
                logging.error(traceback.format_exc())

            except KeyboardInterrupt:
                logging.info("ctrl + c:")
                epd2in7_V2.epdconfig.module_exit(cleanup=True)
                exit()

    def empty_table(self):
        session = SessionLocal()
        try:
            session.query(BatterySchedule).delete()
            session.commit()
            print("Table emptied successfully.")
        except Exception as e:
            session.rollback()
            print(f"Error emptying table: {e}")
        finally:
            session.close()
        


if __name__ == "__main__":

    test = BatteryScada(batt_id="batt-0001", round_trip=0.97)
    test.get_current_state_of_charge()
    #test.empty_table()
    #test.prepare_xls()


    # Connect to the MQTT broker
    mqtt_client = MqttClient("159.89.103.242", 1883, "battery_scada/batt-0001")
    mqtt_client.connect_client()
    # # Create a scheduler instance
    scheduler = BackgroundScheduler()
    
    # # Add a job to the scheduler
    scheduler.add_job(test.update_actual_battery_state_in_db, CronTrigger(minute='*'))  # This runs the job every minute
    scheduler.add_job(test.fetch_actual_db, CronTrigger(minute='*'))  # This runs the job every minute
    
    # scheduler.add_job(test.empty_table, CronTrigger(hour=0, minute=5))
    scheduler.add_job(test.prepare_xls, CronTrigger(hour=17, minute=10))

    scheduler.start()

    try:
        # Keep the main thread alive
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        
        #Shut down the scheduler gracefully
        scheduler.shutdown()
        mqtt_client.disconnect_client()
    




