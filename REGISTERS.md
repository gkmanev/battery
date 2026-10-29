# Modbus RC interface

`battery_scada.py` is a Modbus TCP server on port 5020, using pymodbus
3.8.6 (pinned in requirements.txt). Addresses are zero-based holding register
addresses as transmitted in Modbus requests. Float32 values use big-endian
bytes and the high word first. Write commands with function 16, including
both words of each float32 in one request.

| Address | Name | Type | Units | Direction | Update |
| --- | --- | --- | --- | --- | --- |
| 0–1 | DP_BESS_MV | float32 | MW | RC → BESS | Event |
| 2–3 | DP_WT_MV | float32 | MW | RC → WT | Event |
| 6–7 | P_WT | float32 | MW | WT → RC | 1 s* |
| 8–9 | Q_WT | float32 | MVAr | WT → RC | 1 s* |
| 10–11 | V_WT | float32 | kV | WT → RC | 1 s* |
| 12–13 | F_WT | float32 | Hz | WT → RC | 1 s* |
| 14–15 | SOC_BESS_MV | float32 | % | BESS → RC | 200 ms |
| 16–17 | P_BESS_MV | float32 | MW | BESS → RC | 200 ms |
| 18–19 | Q_BESS_MV | float32 | MVAr | BESS → RC | 200 ms* |
| 20 | SOC_MAX_BESS_MV | uint16 | % | BESS → RC | Available on connect |
| 21 | SOC_MIN_BESS_MV | uint16 | % | BESS → RC | Available on connect |
| 22 | Prated_BESS_MV | uint16 | kW | BESS → RC | Available on connect |
| 23 | Capacity_BESS_MV | uint16 | kWh | BESS → RC | Available on connect |

*The simulator models one MV battery with active power only. Reactive power,
WT telemetry are zero placeholders, not live measurements. WT commands are
stored separately and logged but do not operate equipment or change the MV
command. Integration with WT equipment is required for live WT data.

Only one BESS is defined. Addresses 4–5 and 24 onward are not exposed;
requests accessing them return Modbus exception 02 (Illegal Data Address).
Existing MV and WT addresses are preserved, leaving a gap at 4–5. Read
addresses 0–3 and 6–23 in separate requests rather than spanning the gap.

MV static values default to `[80, 25, 1000, 1000]` at addresses 20–23, with no
scaling. Positive MV power charges the battery; negative power discharges it.
An MV command overrides the schedule until another command is written or the
process restarts. Zero commands stop power and retain the override.

BESS telemetry is published by a separate background loop targeting 200 ms,
so database and Blynk calls do not block Modbus publication. It publishes the
latest simulation state; this is a best-effort Python simulator, not a real-time
measurement guarantee. Database writes and Blynk updates remain every 60 s.

The old layout (scaled uint16 SoC at 14, static values at 2–5) is incompatible.
Restart the server and update all clients together. `modbus_client.py` uses
the shared address definitions and reads float32 SoC and unscaled ratings.
