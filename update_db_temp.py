from database import BatterySchedule, BatteryActualState, SessionLocal
from datetime import datetime



def fetch_db():
    with SessionLocal() as session:
        result = session.query(BatteryActualState).order_by(BatteryActualState.timestamp.desc()).first()
        print(result.battery_state_of_charge_actual)
            



def save_to_db():
    timenow = datetime.now()
    timestamp = timenow.replace(second=0, microsecond=0)
    session = SessionLocal()
    try:
        actual_state_entry = BatteryActualState(
                timestamp = timestamp,
                battery_state_of_charge_actual = 91, 
                last_min_flow = 0,
                invertor_power_actual = 0,              
            )
        session.add(actual_state_entry)
        session.commit()  # Commit the transaction
    except Exception as e:
        session.rollback()  # Rollback in case of an error
        print(f"Error saving status to DB: {e}")
    finally:
        session.close()  # Close the session


if __name__ == "__main__":
    fetch_db()


