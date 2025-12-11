from pymodbus.client import ModbusTcpClient

HOST = "127.0.0.1"
PORT = 5020
SETPOINT_REG = 10
SOC_REG = 0
POWER_REG = 1

client = ModbusTcpClient(host=HOST, port=PORT)
client.connect()

def write_setpoint_kw(p_kw):
    raw = int(p_kw * 10)
    if raw < 0:
        raw += 0x10000
    client.write_register(address=SETPOINT_REG, value=raw)
    print(f"Sent setpoint: {p_kw:.1f} kW")

def read_status():
    rr = client.read_holding_registers(address=SOC_REG, count=2)
    if rr.isError():
        print("Error reading registers")
        return
    
    soc_scaled = rr.registers[0]
    power_scaled = rr.registers[1]

    # signed conversion for power
    if power_scaled >= 0x8000:
        power_scaled -= 0x10000

    soc = soc_scaled / 100.0
    power_kw = power_scaled / 10.0

    print(f"SoC: {soc:.2f}% | Actual power: {power_kw:.1f} kW")

# Test:
write_setpoint_kw(800)   # discharge 300 kW
read_status()

# write_setpoint_kw(500)    # charge 500 kW
# read_status()

client.close()
