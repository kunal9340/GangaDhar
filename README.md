# LCEPL WebSCADA (MQTT)

Local graphical web SCADA starter for the Gangadhar Meher Micro Lift Irrigation project.

## Features
- Graphical dashboard with common header, flow meter, four pump symbols and status colors.
- Pump house cards with vibration, temperature, active power, phase currents and MBFV position.
- Electrical tag table, trends, tag monitor, report history, daily discharge and CSV export.
- Node.js server subscribes to MQTT over TCP and forwards live payloads to the browser using Socket.IO.
- Incoming JSON snapshots are appended to `data/history.jsonl`; daily discharge is stored in `data/daily-discharge.json`.
- India Standard Time display.
- MQTT connection settings are editable in the website and saved locally.

## Run on Windows or macOS
1. Install Node.js LTS.
2. Extract the ZIP to a folder, e.g. `C:\LCEPL_WebSCADA_MQTT`.
3. Open a terminal in the extracted folder and run `npm install`.
4. Run `npm start`.
5. Open `http://localhost:8086`.
6. Go to **MQTT Settings**, enter broker IP/port, username/password, subscribe topic, enable MQTT and click **Save & Connect**.

A Windows helper file `START_SCADA.bat` is included. Double-click it after `npm install` to start the site.

## Payload expected
```json
{
  "timestamp": "2026-10-24T12:50:41",
  "timezone": "Asia/Kolkata",
  "data": [
    {"name":"Header Pressure Transmitter","value":4,"unit":"bar"},
    {"name":"Motor 1 Running Status","value":"RUNNING","unit":""},
    {"name":"Flow Transmitter","value":4572,"unit":"L/s"}
  ]
}
```

The server subscribes to one MQTT topic. Browser MQTT over WebSocket is not required; the server uses TCP MQTT (default port 1883).

## Important engineering notes
- The sample payload is included for UI preview only. Values in it are not live PLC readings.
- Verify broker access, topic direction, credentials, units, scaling, and Modbus/register mapping against the actual PLC configuration.
- The current daily discharge calculation integrates flow samples only when unit is explicitly `L/s` or `m³/h`/`m3/hr`, and ignores intervals longer than one hour. If the flow unit differs, correct the integration before using discharge figures operationally.
- If the server is offline, incoming MQTT messages are not recorded unless the publisher/broker separately retains or buffers them. Saved history survives a normal server restart as long as `data/history.jsonl` and `data/daily-discharge.json` are preserved.
- This is a monitoring interface. No pump control buttons are included.
