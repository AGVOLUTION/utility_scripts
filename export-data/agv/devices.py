"""Python Wrapper / SDK for most important queries to Devices-API"""
import logging
import os

import requests
from datetime import datetime, timedelta
import gql
from gql.transport.aiohttp import AIOHTTPTransport
from gql.dsl import DSLQuery, DSLSchema, dsl_gql
import pandas as pd

logger = logging.getLogger(__name__)

class Client():
    """Represent a GraphQL client instance to Agvolution Devices API.
    """
    def __init__(self, sample_eui, url="https://api.agvolution.com/devices"):
        """Initialize Client.

        Args:
            sample_eui (_type_): The client requires a sample-EUI to fetch the GraphQL description language schema initially.
            url (str, optional): API-Endpoint. Defaults to "https://api.agvolution.com/devices".
        """
        self.cl = None
        self.ds = None
        self.url = url
        self.getAuthToken(input("Enter username: "), input("Enter password: "), sample_eui)
    
    def getAuthToken(self, user, password, sample_eui):
        """Obtain authorization token."""
        body = {
            "email": user,
            "password": password
        }
        r = requests.post('https://api.agvolution.com/auth/session', json=body)
        id_token = r.json()['id_token']

        # Select Agvolution Devices Endpoint. Provide Authorization Header.
        transport = AIOHTTPTransport(url=self.url, headers={'Authorization': id_token})

        # Create a GraphQL client using the defined transport. Fetch the Schema.
        self.cl = gql.Client(transport=transport, fetch_schema_from_transport=True)

        # Execute a simple device query to fetch schema data (client must execute a query, otherwise, it will not fetch the schema after instantiation)
        query = gql.gql(
        """
        query device($id: String!) {
        device(id: $id) {
            id
            latestSignal
        }
        }
        """
        )

        # Execute the query on the transport
        result = self.cl.execute(query, variable_values={'id': sample_eui})

        # Retrieve schema via introspection
        self.ds = DSLSchema(self.cl.schema)
  
    def execute(self, query):
        return self.cl.execute(query)

class Device():
    """Download device meta-information, such as position,
    sensor systems (capabilities including the key, parameter and unit),
    and the latest signal.
    """
    def __init__(self, cl: Client, id: str):
        """Initialize device instance

        Args:
            cl (Client): An instance of agv.devices.Client
            id (str): Device-EUI
        """
        self.cl = cl
        self.device = None
        
        query = dsl_gql(DSLQuery(
            cl.ds.Query.device(id=id).select(
                cl.ds.Device.id,
                cl.ds.Device.latestSignal,
                cl.ds.Device.capabilities.select(
                    cl.ds.DeviceCapabilities.measurements.select(
                        cl.ds.DeviceMeasurement.key,
                        cl.ds.DeviceMeasurement.param,
                        cl.ds.DeviceMeasurement.active
                    )
                )
            )
        ))

        result = cl.execute(query)['device']
        try:
            result['latestSignal'] = datetime.fromisoformat(result['latestSignal'][:-5])
        except Exception as e:
            pass
        self.device = result

    def get(self):
        return self.device


class DeviceTimeseries():
    """Download device data time series for a given Device-EUI.
    The filter object specifies start, end, keys and parameters, which shall be fetched.
    Visit the documentation pages for further information (https://services.agvolution.com).
    """
    def __init__(self, cl: Client, device: str, filter: dict):
        """Initialize Device Timeseries

        Args:
            cl (Client): An instance of agv.devices.Client
            device (str): Device-EUI
            filter (dict): A filter object, containing: start, end, keys, params
        """
        self.cl = cl # Remember client
        dfs = {} # Collect data frames per key|params
        
        # Slice Downloading with 1 month slice sice
        start = datetime.fromisoformat(filter['start'])
        end = datetime.fromisoformat(filter['end'])
        
        terminate = False # Termination flag
        retries = 3 # Number of retries in case of no / empty response
        while not terminate:
            end_slice = start + timedelta(days=30)
            if end_slice > end:
                end_slice = end # Requested download period exceeds today -> Shrink time interval and terminate after last slice
                terminate = True
            print("Downloading slice...", start, end_slice)
            
            # Define a deviceTimeseries Query
            q = DSLQuery(
                cl.ds.Query.deviceTimeseries(device=device, filter=filter).select(
                    cl.ds.DeviceTimeSeriesPaginated.queryUuid,
                    cl.ds.DeviceTimeSeriesPaginated.series.select(
                        cl.ds.DeviceTimeSeries.device,
                        cl.ds.DeviceTimeSeries.lon,
                        cl.ds.DeviceTimeSeries.lat,
                        cl.ds.DeviceTimeSeries.timeseries.select(
                            cl.ds.TimeSeries.param,
                            cl.ds.TimeSeries.key,
                            cl.ds.TimeSeries.unit,
                            cl.ds.TimeSeries.aggregate,
                            cl.ds.TimeSeries.interval,
                            cl.ds.TimeSeries.start,
                            cl.ds.TimeSeries.end,
                            cl.ds.TimeSeries.values.select(
                                cl.ds.TimedValue.time,
                                cl.ds.TimedValue.value
                            )
                        )
                    )
                )
            )
            
            query = dsl_gql(
                q
            )

            try:
                #                             list of time series -> take first
                # series is a list that contains device id, position and TimeSeries objects
                # TimeSeries object for one param | key
                # Select the default deviceTimeSeries
                ts = (cl.execute(query))['deviceTimeseries'][-1]['series']
            except Exception as e:
                logger.error(repr(e))
            
            if ts is None or len(ts) == 0:
                if retries > 0:
                    retries -= 1 # Consume one retry
                    continue
                else:
                    retries = 3 # Continue with next slice
                    start = end_slice
                    continue
                    
            # Concatenate all sub-timeseries
            for timeseries in ts:
                data = timeseries['timeseries']
                for keyParamPair in data:
                    key = keyParamPair['key']
                    param = keyParamPair['param']
                    df = pd.DataFrame.from_records(keyParamPair['values']) # Parse dict
                    df.rename(columns={'value': key + '|' + param}, inplace=True) # Rename value column
                    df['time'] = pd.to_datetime(df['time']) # Parse datetimes
                    df = df.set_index('time')
                    
                    if not key in dfs.keys():
                        dfs[key] = {}

                    if param in dfs[key].keys(): # There is already a dataframe with this key|param label
                        dfs[key][param] = pd.concat(
                            [dfs[key][param], df],
                            axis=0,
                            join='inner') # Append
                        mask = ~dfs[key][param].index.duplicated() # Remove duplicate indices which might occur at concatenation interfaces
                        dfs[key][param] = dfs[key][param][mask]
                    else:
                        dfs[key][param] = df # Create new
                        
            start = end_slice
        
        self.ts = dfs
            
    def getSingleFrames(self):
        # Combine data
        return self.ts
    
    def getMergedFrame(self, fillna=True):
        # Concatenate to a single Data Frame
        merged_df = None
        for key in self.ts.keys():
            for param in self.ts[key].keys():
                df = self.ts[key][param]
                if merged_df is None:
                    merged_df = df
                else:
                    merged_df = merged_df.join(df, how='outer')
        if fillna:
            merged_df = merged_df.fillna(method='ffill')
        return merged_df
                    
    def plot(self):
        merged_df = self.getMergedFrame()
        merged_df.plot(subplots=True, style='-')