import pandas as pd
import csv

with open('air_data_stations.csv', newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        df = pd.date_range(start='2017-01-01 23:00:00', freq='D', periods=365)
        dates = df.to_native_types()
        with open('2017_'+ row['id'] + '.csv','w+', newline='') as f:
            fieldNames = ['datetime', 'id', 'name', 'longitude', 'latitude', 'live', 'cityid', 'stateid']
            writer = csv.DictWriter(f, fieldnames=fieldNames)
            writer.writeheader()
            for date in dates:
                day = date[:10] + 'T' + date[11:] + 'Z'
                writer.writerow({'datetime' : day, 'id' : row['id'] , 'name' : row['name'], 'longitude' : row['longitude'], 'latitude': row['latitude'], 'live': row['live'], 'cityid': row['cityID'], 'stateid': row['stateID']})

        