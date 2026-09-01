import time
import struct
from pymodbus.client import ModbusTcpClient

#HOST = "85.14.6.37"
HOST = "192.168.1.4"
PORT = 5020
#PORT = 16598
DP_BESS_MV_ADDR = 0  # float32 MW at HR0/1 (setpoint)
SOC_REG = 14      # uint16 % * 100 at HR14
POWER_REG = 16    # float32 MW at HR16/17
RATED_POWER_REG = 2
CAPACITY_REG = 3
SOC_MIN_REG = 4
SOC_MAX_REG = 5
# Set this to the desired initial setpoint (MW) to send once on connect.
DP_BESS_MV = 0.25

client = ModbusTcpClient(host=HOST, port=PORT)
client.connect()

def read_ratings():
    rr = client.read_holding_registers(address=RATED_POWER_REG, count=4)
    if rr.isError():
        print("Error reading rated power/capacity/SOC limit registers")
        return

    Prated_BESS_MV = rr.registers[0] / 10.0
    Capacity_BESS_MV = rr.registers[1] / 10.0
    SOC_MIN_BESS_MV = rr.registers[2] / 100.0
    SOC_MAX_BESS_MV = rr.registers[3] / 100.0

    print(
        f"Prated_BESS_MV: {Prated_BESS_MV:.1f} kW | "
        f"Capacity_BESS_MV: {Capacity_BESS_MV:.1f} kWh | "
        f"SOC_MIN_BESS_MV: {SOC_MIN_BESS_MV:.2f}% | "
        f"SOC_MAX_BESS_MV: {SOC_MAX_BESS_MV:.2f}%"
    )

def _float_to_regs(value: float):
    packed = struct.pack(">f", float(value))
    hi, lo = struct.unpack(">HH", packed)
    return [hi, lo]

def _regs_to_float(regs, index=0):
    if len(regs) < index + 2:
        return 0.0
    hi, lo = regs[index], regs[index + 1]
    return struct.unpack(">f", struct.pack(">HH", hi, lo))[0]

def write_dp_bess_mv(p_mw):
    regs = _float_to_regs(p_mw)
    client.write_registers(address=DP_BESS_MV_ADDR, values=regs)
    print(
        f"Sent DP_BESS_MV: {p_mw:.3f} MW (HR{DP_BESS_MV_ADDR}/HR{DP_BESS_MV_ADDR + 1})"
    )

def read_status():
    rr = client.read_holding_registers(address=SOC_REG, count=4)
    if rr.isError():
        print("Error reading registers")
        return
    
    soc_scaled = rr.registers[0]
    SOC_BESS_MV = soc_scaled / 100.0

    P_BESS_MV = _regs_to_float(rr.registers, index=2)

    print(f"SOC_BESS_MV: {SOC_BESS_MV:.2f}% | P_BESS_MV: {P_BESS_MV:.3f} MW")

# Poll loop (every 1 second)
read_ratings()
write_dp_bess_mv(DP_BESS_MV)
try:
    while True:
        read_status()
        time.sleep(1)
except KeyboardInterrupt:
    print("Stopping polling.")
finally:
    client.close()
