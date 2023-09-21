import csv
from datetime import datetime
import pytz

def read_csv_and_update_datetime(input_file, output_file):
    with open(input_file, 'r', newline='') as csv_file:
        reader = csv.DictReader(csv_file)
        
        # Define the column names you want to update (replace with your column names)
        columns_to_update = ['startTime', 'endTime']
        
        rows = []
        for row in reader:
            for column in columns_to_update:
                print(row[column])
                if column in row and row[column]:  # Check if column exists and is not empty
                    try:
                        # Parse the datetime string with milliseconds and timezone offset
                        datetime_obj = datetime.strptime(row[column], '%Y-%m-%dT%H:%M:%S.%f%z')
                        
                        # Convert datetime to another timezone (e.g., GMT+10)
                        gmt10 = pytz.timezone('Australia/Sydney')
                        updated_datetime = datetime_obj.astimezone(gmt10)
                        
                        # Format the updated datetime string as desired
                        row[column] = updated_datetime.strftime('%Y-%m-%d %H:%M:%S')
                    except ValueError as e:
                        print(f"Error updating datetime format for {column}: {e}")
            rows.append(row)

    with open(output_file, 'w', newline='') as csv_out:
        fieldnames = reader.fieldnames
        writer = csv.DictWriter(csv_out, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

# Specify the input and output file paths
input_file_path = 'input.csv'  # Replace with your input CSV file
output_file_path = 'output.csv'  # Replace with desired output file name

# Call the function to read CSV and update datetime format
read_csv_and_update_datetime(input_file_path, output_file_path)
