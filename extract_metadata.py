import os
import json
import csv
from datetime import datetime, timedelta
import pytz
import requests
import PureCloudPlatformClientV2
from PureCloudPlatformClientV2.rest import ApiException

# Function to extract JSON data from a JSON file and return it as a dictionary
def extract_json_data(json_file_path):
    with open(json_file_path, 'r') as file:
        data = json.load(file)
    return data

# Function to convert a datetime string to GMT+10 timezone
def convert_datetime_to_gmt10(datetime_str):
    try:
        input_datetime = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        gmt10 = pytz.timezone('GMT+10')
        return input_datetime.replace(tzinfo=pytz.utc).astimezone(gmt10)
    except ValueError:
        return datetime_str  # Return the original string if it's not a valid datetime

# Function to convert a duration in milliseconds (ms) to hh:mm:ss format
def convert_ms_to_hhmmss(duration_ms):
    try:
        seconds = int((duration_ms / 1000) % 60)
        minutes = int((duration_ms / (1000 * 60)) % 60)
        hours = int((duration_ms / (1000 * 60 * 60)) % 24)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    except ValueError:
        return duration_ms  # Return the original value if it's not a valid duration

# Function to get user details by ID from PureCloud API
def get_user_details_by_id(user_id):
    # Credentials
    CLIENT_ID = ''
    CLIENT_SECRET = ''
    ORG_REGION = ''  # eg. us_east_1

    # Set environment
    region = PureCloudPlatformClientV2.PureCloudRegionHosts[ORG_REGION]
    PureCloudPlatformClientV2.configuration.host = region.get_api_host()

    try:
    # OAuth when using Client Credentials
        api_client = PureCloudPlatformClientV2.ApiClient().get_client_credentials_token(CLIENT_ID, CLIENT_SECRET)
        api_instance = PureCloudPlatformClientV2.UsersApi(api_client)
    # user_id = user_id # str | User ID
    # expand = ['expand_example'] # list[str] | Which fields, if any, to expand (optional)
    # integration_presence_source = 'integration_presence_source_example' # str | Gets an integration presence for a user instead of their default. (optional)
    # state = ''active'' # str | Search for a user with this state (optional) (default to 'active')
        api_response = api_instance.get_user(user_id)
        print(api_response.name)
        return api_response.name
    except Exception as e:
        # print(f"Error fetching user details: {str(e)}")
        return 'User not found'

# Function to recursively traverse the JSON data, convert datetime strings and durations,
# and fetch user details
# Function to recursively traverse the JSON data, convert datetime strings and durations,
# and fetch user details
def convert_data_in_json_and_fetch_users(data):
    keys_to_update = []  # Create a list to store keys that need to be modified

    if isinstance(data, dict):
        for key, value in data.items():
            if key == 'userIds':
                user_ids = value if isinstance(value, list) else [value]
                last_names = []
                for user_id in user_ids:
                    last_name = get_user_details_by_id(user_id)
                    last_names.append(last_name)
                keys_to_update.append(('userNames', last_names))
            data[key] = convert_data_in_json_and_fetch_users(value)
    
        # Update the dictionary outside the loop
        for key, new_value in keys_to_update:
            data[key] = new_value
    elif isinstance(data, list):
        for i, item in enumerate(data):
            data[i] = convert_data_in_json_and_fetch_users(item)
    elif isinstance(data, str):
        data = convert_datetime_to_gmt10(data)  # Convert datetime strings
    elif isinstance(data, int):
        data = convert_ms_to_hhmmss(data)  # Convert duration in milliseconds
    return data


# Function to write data to a CSV file using JSON keys as headers
def write_json_to_csv(csv_file_path, data_list):
    if not data_list:
        return

    # Get the keys from the first JSON object to use as CSV headers
    headers = list(data_list[0].keys())

    with open(csv_file_path, 'w', newline='') as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=headers)
        csv_writer.writeheader()  # Write headers

        for data in data_list:
            csv_writer.writerow(data)  # Write data

# Specify the directory containing JSON files
directory_path = '/Recordings/'

# Specify the CSV file where data will be stored
csv_file_path = 'output.csv'

json_data_list = []

# Traverse the directory and its subdirectories
for root, dirs, files in os.walk(directory_path):
    for file_name in files:
        if file_name.endswith('.json'):
            json_file_path = os.path.join(root, file_name)
            json_data = extract_json_data(json_file_path)
            json_data = convert_data_in_json_and_fetch_users(json_data)  # Convert datetime strings, durations, and fetch users
            json_data_list.append(json_data)

# Write JSON data to the CSV file with keys as headers
write_json_to_csv(csv_file_path, json_data_list)