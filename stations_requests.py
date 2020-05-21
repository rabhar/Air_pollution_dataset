import csv
import requests
import base64
import time
import datetime
import concurrent.futures
import os
import re


def make_hit(row):
    dataDict = '{"station_id":"' + row['id'] + '","date":"' + row['datetime'] + '"}'
    data = base64.b64encode(str(dataDict).encode()).decode()
    
    seconds = int(time.time()*1000)
    offset = int((datetime.datetime.utcnow() - datetime.datetime.now()).total_seconds()//60)

    token = '{"time":' + str(seconds) + ',"timeZoneOffset":'+ str(offset) +'}'
    accessToken = base64.b64encode(str(token).encode()).decode()
    
    headers = {
        'authority': 'app.cpcbccr.com',
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accesstoken': accessToken,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'origin': 'https://app.cpcbccr.com',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': 'https://app.cpcbccr.com/AQI_India/',
        'accept-language': 'en-US,en;q=0.9'
    }
    
    response = requests.post('https://app.cpcbccr.com/aqi_dashboard/aqi_all_Parameters', headers=headers, data=data, verify=False)
    return row, response


with open("error.csv",'a+') as errorFile:
    errorFieldNames = ['datetime', 'id', 'text', 'status_code']
    errorwriter = csv.DictWriter(errorFile, fieldnames=errorFieldNames)
    errorwriter.writeheader()
    files = os.listdir()
    for inputFile in files:
        if re.match('^2017_.*', inputFile):        
            with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
                with open(inputFile) as readFile:
                    with open('output/output_' + inputFile , 'a+', newline='') as outputFile:
                        
                        fieldNames = ['datetime', 'id', 'name', 'longitude', 'latitude', 'live', 'cityid', 'stateid', 'PM2.5', 'PM10', 'NO2', 'NH3', 'SO2', 'CO', 'OZONE']
                        
                        writer = csv.DictWriter(outputFile, fieldnames=fieldNames)
                        writer.writeheader()
                        reader = csv.DictReader(readFile)
                        
                        future_to_row = {executor.submit(make_hit, row) : row for row in reader}
                        
                        processed_row = 0
                        
                        for future in concurrent.futures.as_completed(future_to_row):
                            try:
                                inputRow, response = future.result()
                                if response.status_code == 200:
                                    j = response.json()
                                    if 'chartData' in j:
                                        
                                        for i in range(1,len(j['chartData'][0])):
                                            d = {
                                                'datetime' : j['chartData'][0][i][0],
                                                'id' : inputRow['id'],
                                                'name' : inputRow['name'],
                                                'longitude' : inputRow['longitude'],
                                                'latitude' : inputRow['latitude'],
                                                'live' : inputRow['live'],
                                                'cityid' : inputRow['cityid'],
                                                'stateid' : inputRow['stateid']
                                            }
                                            for rowno,params in enumerate(j['metrics']):
                                                d[params['name']] = j['chartData'][rowno][i][1]
                            
                                            writer.writerow(d)
                                            processed_row += 1
                                            print(processed_row)
                                else:
                                    errorwriter.writerow({'datetime' : inputRow['datetime'], 'id': inputRow['id'], 'text': str(exc), 'status_code' : str(response.status_code)})
                            except Exception as exc:
                                errorwriter.writerow({'datetime' : inputRow['datetime'], 'id': inputRow['id'], 'text': str(exc), 'status_code' : str(response.status_code)})

                        
                    
                