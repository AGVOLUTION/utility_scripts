from matplotlib import pyplot as plt
import json
from datetime import date, datetime, timedelta
import pandas as pd
from agv.devices import Device, DeviceTimeseries, Client
import os
from pprint import pprint

if __name__ == '__main__':
    # Load the event, which describes which EUIs shall be downloaded
    with open('event.json', 'r') as eventfile:
        event = json.load(eventfile)
        
    # The client instance requires a sample EUI -> Use the first available
    cl = Client(event['euis'][0])
        
    # Create export directory if not exists
    try:
        os.mkdir('export')
    except Exception as e:
        pass
        
    # Process each sensor EUI separately
    for eui in event['euis']:
        # Also fetch the Device record
        device = Device(cl, eui)
        pprint(device.get())
        
        ts = DeviceTimeseries(cl, eui,
        {
            "start": event['start'],
            "end": event['end'],
            "keys": event['keys'],
            "params": event['params']
        })
        
        # Print Data Frame
        pprint(ts.getSingleFrames())
        frame = ts.getMergedFrame()

        # Overview plot
        if event['plot']:
            frame.plot(subplots=True, sharex=True, title=eui, style='-', figsize=(7,9))
            plt.savefig(f'export/{eui}.png', bbox_inches='tight')
            plt.show()
            
        # Save data to CSV file
        frame.to_csv(f'export/{eui}.csv')