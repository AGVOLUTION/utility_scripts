# utility_scripts
A collection of helper scripts, e.g. for exporting sensor data.

## export-data
This subfolder provides a Python Wrapper to access IoT device data via the GraphQL API (see: https://services.agvolution.com). In the file export-data.py, you will find example code how to use the Python classes in agv.devices. It contains three classes:
- Client: A GraphQL client wrapper.
- Device: Fetch device metadata, like position, capabilities (installed sub-sensor systems) and the time of the latest signal.
- DeviceTimeseries: Fetch device timeseries data (parameter vs. time as point-like data). The result will be returned as a Pandas DataFrame object, and can then easily be used for further processing, plotting or exporting purposes.

### How to select devices, parameters and the time period?
The devices, parameters and time period of the export are configured in event.json. 

### How is the data represented in Python?
Timeseries-data is returned as a merged or multiple single Pandas DataFrames (https://pandas.pydata.org/docs/index.html).

The column labels of the **merged frame** have the naming scheme "KEY|PARAM" (e.g. KEY: +15cm, PARAM: ENV__ATMO__T -> This would represent an air temperature sensor at +15 cm height).

**Single data frames** are kept in a dict as frames[KEY][PARAM].
