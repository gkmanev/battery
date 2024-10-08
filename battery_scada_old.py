import pandas as pd
import mqtt_client
import json
import datetime
import time
import sys
import os
import xlrd
import traceback
import time
import logging
from database import BatterySchedule, SessionLocal
from sqlalchemy import Column, Integer, String, Float, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta,date
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
# from PIL import Image, ImageDraw, ImageFont
# from waveshare_epd import epd2in7_V2


logging.basicConfig(level=logging.DEBUG)

class BatteryScada():
    def __init__(self, schedule_file) -> None:
        self.filename = schedule_file
        self.state_of_charge = 0
        self.schedule = 0
        self.battery_state = "Idle"
        self.accumulate_charge_rounded = 0
        self.excel_workbook = None

    def open_xls_file(self):
        self.excel_workbook = xlrd.open_workbook(self.filename)
    


    def prepare_xls(self):
        try:            
            self.excel_workbook = xlrd.open_workbook(self.filename)            
            excel_worksheet = self.excel_workbook.sheet_by_index(0)
            xl_date = date.today()
            xl_date_time = str(xl_date) + "T01:15:00"
            period = (24 * 4) + 4
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
           #self.prepare_and_send_status(df)
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


    def fetch_all_statuses(self):
        # Get the database session
        session = SessionLocal()
        
        try:
            # Query all rows in the BatteryStatus table
            all_statuses = session.query(BatterySchedule).all()
            
            # Iterate over each status and print it
            for status in all_statuses:
                print(f"ID: {status.id}, Timestamp: {status.timestamp}, "
                      f"Battery State: {status.battery_state}, "
                      f"Schedule: {status.schedule}, "
                      )
        
        except Exception as e:
            print(f"Error fetching statuses: {e}")
        
        finally:
            session.close()  # Close the session


    # def prepare_and_send_status(self, df):                         
    #     for row in df.itertuples():
    #         timenow = datetime.now()
    #         quarter_min = self.lookup_quarterly(timenow.minute)                           
    #         if quarter_min == 0:
    #             quarter_hour = timenow.hour + 1
    #         else:
    #             quarter_hour = timenow.hour                     
    #         schedule_hour = row.Index.hour
    #         schedule_min = row.Index.minute
    #         if quarter_hour == schedule_hour and schedule_min == quarter_min:          
                                                      
    #             self.schedule = row.schedule                               
    #             self.state_of_charge += self.schedule
    #             self.accumulate_charge_rounded = float(f"{round(self.state_of_charge / 60, 2):.2f}")                  
    #             if self.schedule > 0:
    #                 self.battery_state = "Charging"
    #             elif self.schedule < 0:
    #                 self.battery_state = "Discharging"
    #             else:
    #                 self.battery_state = "Idle"               

    #             status_obj = {
    #                 self.battery_state:self.schedule,
    #                 "SoC": self.accumulate_charge_rounded     
           
    #             }
    #             mqtt_client.publish_message(str(status_obj))
    #             print(f"sched_hour={schedule_hour}:{schedule_min} || quarter_hour={quarter_hour}:{quarter_min} || Real Time:{timenow.hour}:{timenow.minute} || schedule:{self.schedule}")                
            
        
    # def lookup_quarterly(self, minutes):

    #     if 0 <= minutes <= 14:
    #         return 15
    #     elif 15 <= minutes <= 29:
    #         return 30
    #     elif 30 <= minutes <= 44:
    #         return 45
    #     elif 45 <= minutes <= 59:
    #         return 0
    #     else:
    #         raise ValueError("Minutes must be between 0 and 59")
        
        
    # def display_data(self):
    #     script_dir = os.path.dirname(os.path.realpath(__file__))
    #     picdir = os.path.join(script_dir, 'pic')
    #     libdir = os.path.join(script_dir, 'lib')
    #     if os.path.exists(libdir):
    #         sys.path.append(libdir)
    #     try:
    #         epd = epd2in7_V2.EPD()
    #         epd.init()
    #         epd.Clear()
    #         font_path = os.path.join(picdir, 'Font.ttc')
    #         try:
    #             font24 = ImageFont.truetype(font_path, 24)
    #             font20 = ImageFont.truetype(font_path, 18)
    #         except IOError as e:                
    #             font24 = ImageFont.load_default()  # Use default font if the specified font is not found
    #             font20 = ImageFont.load_default()
    #         # Prepare the image with horizontal orientation    
    #         image = Image.new('1', (epd.height, epd.width), 255)  # 255: clear the frame
    #         draw = ImageDraw.Draw(image)
            
    #         current_time = time.strftime('%d-%m-%Y %H:%M')
    #         cell_width = 80
    #         cell_height = 40
    #         # Clear the entire image
    #         draw.rectangle((0, 0, epd.height, epd.width), fill=255)
            
    #         draw.rectangle((0, 0, cell_width, cell_height), outline=0)
    #         # Draw the current time at the top left corner
    #         draw.text((8, 10), "Battery1", font=font20, fill=0)
            
    #         draw.rectangle((cell_width, 0, cell_width * 2+20, cell_height), outline=0)
    #         # Draw "100MW" next to the time with adjusted spacing
    #         draw.text((90, 10), "100MW/h", font=font20, fill=0)
            
    #         draw.rectangle((cell_width, 0, cell_width * 3 +20, cell_height), outline=0)


    #         # Draw "25MW" next to "100MW" with adjusted spacing
    #         draw.text((190, 10), "25MW", font=font20, fill=0)
            
    #         draw.text((8, 45), current_time, font=font20, fill=0)
            
    #         draw.text((8, 90), f"SoC: {self.accumulate_charge_rounded} MW/h", font=font20, fill=0)
    #         draw.text((8, 120), f"{self.battery_state}: {self.schedule} MW", font=font20, fill=0)

    #         # Perform a full update            
    #         epd.display(epd.getbuffer(image))
    #     except IOError as e:
    #         logging.info(e)
    #         logging.error(traceback.format_exc())

    #     except KeyboardInterrupt:
    #         logging.info("ctrl + c:")
    #         epd2in7_V2.epdconfig.module_exit(cleanup=True)
    #         exit()

    def reset_soc_every_day(self):
        self.state_of_charge = 0
        


if __name__ == "__main__":

    test = BatteryScada("schedule_1.xls")
    test.fetch_all_statuses()


    # Connect to the MQTT broker
    # mqtt_client.connect_client()
    # # Create a scheduler instance
    # scheduler = BackgroundScheduler()
    # battery = BatteryScada("schedule_1.xls")
    # # Add a job to the scheduler
    # scheduler.add_job(battery.prepare_xls, CronTrigger(minute='*'))  # This runs the job every minute
    # scheduler.add_job(battery.display_data, CronTrigger(minute='*'))  # This runs the job every minute
    # scheduler.add_job(battery.reset_soc_every_day, CronTrigger(hour=0, minute=0))

    # scheduler.start()

    # try:
    #     # Keep the main thread alive
    #     while True:
    #         time.sleep(1)
    # except (KeyboardInterrupt, SystemExit):
    #     # Shut down the scheduler gracefully
    #     scheduler.shutdown()
    #     mqtt_client.disconnect_client()
    
    # # Disconnect from the MQTT broker
    # mqtt_client.disconnect_client()



