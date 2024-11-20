import pandas as pd

file_name = 'data_group_3'
file_path = f'provided_files/{file_name}.csv'
data = pd.read_csv(file_path)

timestamp_column = 'timestamp'
data[timestamp_column] = pd.to_datetime(data[timestamp_column])
data['date'] = data[timestamp_column].dt.strftime('%Y_%m_%d')

for date, group in data.groupby('date'):
    group = group.drop(columns=['date']) 
    output_location = f"{file_name}_{date}.csv"
    group.to_csv(output_location, index=False)
    print(f"File saved: {output_location}")
