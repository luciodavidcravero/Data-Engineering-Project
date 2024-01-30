'''
This file generates a basic connection to Carbon Intensity API
https://carbon-intensity.github.io/api-definitions/?python#carbon-intensity-api-v2-0-0
'''

#--------------------------------------------------------------------------
#Import required libraries
#--------------------------------------------------------------------------
import pandas as pd
import requests
from datetime import datetime, timedelta

#--------------------------------------------------------------------------
#API connection (test for only 1 day)
#--------------------------------------------------------------------------
headers = {
  'Accept': 'application/json'
}

base_url = 'https://api.carbonintensity.org.uk/intensity'
endpoint = 'stats'
from_date = '2024-01-01T00:00Z'
to_date = '2024-01-29T23:59Z'
from_date = datetime.fromisoformat(to_date[:-1]) - timedelta(days=30)
from_date = from_date.strftime('%Y-%m-%dT%H:%MZ')
block_date = 24
full_url = f"{base_url}/{endpoint}/{from_date}/{to_date}/{block_date}"
r = requests.get(full_url,
                 params={},
                 headers = headers)

#--------------------------------------------------------------------------
#Save data into variable
#--------------------------------------------------------------------------
#json_response = r.json()
#json_response.keys()

if r.status_code == 200:
    print("Success!")
    json_data = r.json()['data']
    df_carbon_intensity = pd.json_normalize(json_data)
else:
    print("Failure!")
    print(r.status_code)
    print(r.text)
    
#--------------------------------------------------------------------------
#Create table from last 6 months
#--------------------------------------------------------------------------
#Get current date
current_date = datetime.now()

#Calculate dates
from_date = current_date - timedelta(days=180)
from_date_str = from_date.strftime('%Y-%m-%dT%H:%MZ')
to_date_str = current_date.strftime('%Y-%m-%dT%H:%MZ')

block_date = 24

#Empty df to save each iteration data
dfs = []

#Loop to get data in 30 days intervals (API limit)
while from_date < current_date:
    to_date = from_date + timedelta(days=30)
    to_date_str = to_date.strftime('%Y-%m-%dT%H:%MZ')
    
    full_url = f"{base_url}/{endpoint}/{from_date_str}/{to_date_str}/{block_date}"
    
    r = requests.get(full_url,
                     params={},
                     headers=headers)
    
    if r.status_code == 200:
        print(f"Success for {from_date_str} to {to_date_str}")
        json_data = r.json()['data']
        df_carbon_intensity = pd.json_normalize(json_data)
        dfs.append(df_carbon_intensity)
    else:
        print(r.status_code)
        print(r.text)

    #Next interval
    from_date = to_date + timedelta(days=1)
    from_date_str = from_date.strftime('%Y-%m-%dT%H:%MZ')

#Concatenate all df in one
df_carbon_intensity = pd.concat(dfs, ignore_index=True)

#Save 6 months data to csv file
df_carbon_intensity.to_csv('../../data/raw/df_carbon_intensity.csv')