from database import BatterySchedule, BatteryActualState, SessionLocal
from datetime import datetime



def save_to_db():
    timenow = datetime.now()
    timestamp = timenow.replace(second=0, microsecond=0)
    session = SessionLocal()
    try:
        actual_state_entry = BatteryActualState(
                timestamp = timestamp,
                battery_state_of_charge_actual = 59.4, 
                last_min_flow = -0.4,
                invertor_power_actual = -24.25,              
            )
        session.add(actual_state_entry)
        session.commit()  # Commit the transaction
    except Exception as e:
        session.rollback()  # Rollback in case of an error
        print(f"Error saving status to DB: {e}")
    finally:
        session.close()  # Close the session


if __name__ == "__main__":
    save_to_db()