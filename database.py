from sqlalchemy import Column, Integer, String, Float, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# Base class for declarative models
Base = declarative_base()

# Define the battery_status model
class BatterySchedule(Base):
    __tablename__ = 'battery_status'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.now())
    battery_state = Column(String)
    schedule = Column(Float)

class BatteryActualState(Base):
    __tablename__ = 'battery_actual'
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.now())
    battery_state_of_charge_actual = Column(Float)
    last_min_flow = Column(Float)
    invertor_power_actual = Column(Float)
    
    def __setattr__(self, key, value):
        if isinstance(value, float):
            value = round(value, 2)
        super().__setattr__(key, value)



    

# Initialize SQLAlchemy engine and session
engine = create_engine('sqlite:///battery_status.db')
Base.metadata.create_all(engine)  # Create the table(s) if they don't exist

# Create a configured "Session" class
SessionLocal = sessionmaker(bind=engine)
