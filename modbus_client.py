import time
import struct
from pymodbus.client import ModbusTcpClient
from pymodbus.exceptions import ModbusException
import modbus_registers as registers

HOST = "85.14.6.37"
#HOST = "192.168.1.4"
#PORT = 5020
PORT = 16603
DP_BESS_MV_ADDR = registers.DP_BESS_MV
# Set this to the desired initial setpoint (MW) to send once on connect.
DP_BESS_MV = 0.25
# WT commands are optional; None leaves the WT command unchanged.
DP_WT_MV = None

client = ModbusTcpClient(host=HOST, port=PORT)


def _read_registers(address, count):
    response = client.read_holding_registers(address=address, count=count)
    if response.isError():
        print(f"Error reading HR{address}-{address + count - 1}: {response}")
        return None
    if len(response.registers) != count:
        print(f"Incomplete response for HR{address}-{address + count - 1}")
        return None
    return response.registers

def read_ratings():
    words = _read_registers(registers.SOC_MAX_BESS_MV, 4)
    if words is None:
        return

    SOC_MAX_BESS_MV, SOC_MIN_BESS_MV, Prated_BESS_MV, Capacity_BESS_MV = words

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
        raise ValueError("A float32 requires two registers")
    hi, lo = regs[index], regs[index + 1]
    return struct.unpack(">f", struct.pack(">HH", hi, lo))[0]

def _write_setpoint(address, name, p_mw):
    response = client.write_registers(address=address, values=_float_to_regs(p_mw))
    if response.isError():
        print(f"Error writing {name} at HR{address}-{address + 1}: {response}")
        return False
    print(f"Sent {name}: {p_mw:.3f} MW (HR{address}-{address + 1})")
    return True


def write_dp_bess_mv(p_mw):
    return _write_setpoint(DP_BESS_MV_ADDR, "DP_BESS_MV", p_mw)


def write_dp_wt_mv(p_mw):
    return _write_setpoint(registers.DP_WT_MV, "DP_WT_MV", p_mw)


def read_commands():
    # HR4-5 is unavailable: do not include it in a contiguous read.
    words = _read_registers(registers.DP_BESS_MV, 4)
    if words is None:
        return
    print(
        f"DP_BESS_MV: {_regs_to_float(words):.3f} MW | "
        f"DP_WT_MV: {_regs_to_float(words, 2):.3f} MW (stored only)"
    )


def read_wt_status():
    words = _read_registers(registers.P_WT, 8)
    if words is None:
        return
    print(
        f"WT placeholders | P_WT: {_regs_to_float(words):.3f} MW | "
        f"Q_WT: {_regs_to_float(words, 2):.3f} MVAr | "
        f"V_WT: {_regs_to_float(words, 4):.3f} kV | "
        f"F_WT: {_regs_to_float(words, 6):.3f} Hz"
    )

def read_status():
    words = _read_registers(registers.SOC_BESS_MV, 6)
    if words is None:
        return
    
    SOC_BESS_MV = _regs_to_float(words, index=0)
    P_BESS_MV = _regs_to_float(words, index=2)
    Q_BESS_MV = _regs_to_float(words, index=4)

    print(f"SOC_BESS_MV: {SOC_BESS_MV:.2f}% | P_BESS_MV: {P_BESS_MV:.3f} MW | Q_BESS_MV: {Q_BESS_MV:.3f} MVAr")

def main():
    try:
        if not client.connect():
            print(f"Could not connect to {HOST}:{PORT}")
            return
        read_ratings()
        if DP_BESS_MV is not None:
            write_dp_bess_mv(DP_BESS_MV)
        if DP_WT_MV is not None:
            write_dp_wt_mv(DP_WT_MV)
        next_tick = time.monotonic()
        next_wt_tick = next_tick
        while True:
            read_status()
            if time.monotonic() >= next_wt_tick:
                read_commands()
                read_wt_status()
                next_wt_tick = time.monotonic() + 1.0
            next_tick += registers.BESS_UPDATE_INTERVAL_S
            delay = next_tick - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            else:
                next_tick = time.monotonic()
    except KeyboardInterrupt:
        print("Stopping polling.")
    except (ModbusException, OSError) as exc:
        print(f"Modbus communication failed: {exc}")
    finally:
        client.close()


if __name__ == "__main__":
    main()
