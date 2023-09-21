from datetime import datetime
import os
import sys
import time
import PureCloudPlatformClientV2
from PureCloudPlatformClientV2.rest import ApiException
import csv
import requests
import pytz


class OAuthManager:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret

    def get_api_client(self):
        # Set environment
        region = PureCloudPlatformClientV2.PureCloudRegionHosts[ORG_REGION]
        PureCloudPlatformClientV2.configuration.host = region.get_api_host()

        # OAuth when using Client Credentials
        api_client = PureCloudPlatformClientV2.api_client.ApiClient(
        ).get_client_credentials_token(CLIENT_ID, CLIENT_SECRET)
        return api_client


class ConversationDetailsExtractor:
    def __init__(self, api_client):
        self.analytics_api_instance = PureCloudPlatformClientV2.AnalyticsApi(
            api_client)
        self.conversation_api_instance = PureCloudPlatformClientV2.ConversationsApi(
            api_client)
        self.users_instance = PureCloudPlatformClientV2.UsersApi(api_client)
        self.recording_instance = PureCloudPlatformClientV2.RecordingApi(
            api_client)

    def conversation_exits(self, id):
        convo = self.conversation_api_instance.get_conversation(id)
        if (convo.id):
            print(convo.state)
            return True
        else:
            return False

    def log_exception_to_file(self, exception, custom_message=None, log_file_path='exception_logs.txt'):
        try:
            # Combine the custom message and exception information
            if custom_message:
                log_message = f"{custom_message}\n{str(exception)}\n"
            else:
                log_message = f"{str(exception)}\n"

            # Open the log file in append mode and write the log message
            with open(log_file_path, 'a') as log_file:
                log_file.write(log_message)

        except Exception as log_error:
            print(f"Error logging exception: {str(log_error)}")

    def change_timezone(self, datetime_string, source_timezone_str, target_timezone_str):
        try:
            # Convert the string to a datetime object
            datetime_obj = datetime.strptime(
                datetime_string, '%Y-%m-%d %H:%M:%S.%f%z')

            # Define the source and target time zones
            source_timezone = pytz.timezone(source_timezone_str)
            target_timezone = pytz.timezone(target_timezone_str)

            # Convert the datetime to the target timezone
            datetime_obj = datetime_obj.replace(tzinfo=source_timezone)
            target_datetime = datetime_obj.astimezone(target_timezone)

            # Format the result as a string
            target_datetime_string = target_datetime.strftime(
                '%Y-%m-%d %H:%M:%S.%f%z')

            return target_datetime_string
        except Exception as e:
            return str(e)

    def update_progress(self, progress, bar_length=50):
        arrow = '=' * int(round(bar_length * progress))
        spaces = ' ' * (bar_length - len(arrow))
        sys.stdout.write(f'\r[{arrow + spaces}] {int(progress * 100)}%')
        sys.stdout.flush()

    def check_make_dir(self, folder_hierarchy):
        # Split the folder hierarchy into individual folder names
        folder_names = folder_hierarchy.split('/')

        # Initialize a base path (change this to your desired base directory)
        base_path = "media/"

        # Create the folder hierarchy
        current_path = base_path
        for folder_name in folder_names:
            current_path = os.path.join(current_path, folder_name)
            if not os.path.exists(current_path):
                os.makedirs(current_path)
        return current_path

    def extract_day(self, datetime_string):
        datetime_obj = datetime.strptime(
            datetime_string, "%Y-%m-%dT%H:%M:%S.%fZ")
        # Extract month and year
        month = datetime_obj.strftime("%d")  # Full month name
        return month

    def extract_month(self, datetime_string):
        datetime_obj = datetime.strptime(
            datetime_string, "%Y-%m-%dT%H:%M:%S.%fZ")
        # Extract month and year
        month = datetime_obj.strftime("%m")  # Full month name
        return month

    def extract_year(self, datetime_string):
        datetime_obj = datetime.strptime(
            datetime_string, "%Y-%m-%dT%H:%M:%S.%fZ")
        # Extract month and year
        year = datetime_obj.strftime("%Y")
        return year

    def downloadMedia(self, download_batch):
        # Dictionary mapping content types to file extensions
        content_type_to_extension = {
            'audio/mpeg': '.mp3',
            'audio/wav': '.wav',
            'audio/ogg': '.ogg',
            'audio/x-m4a': '.m4a',
            'audio/x-ms-wma': '.wma',
            'audio/webm': '.webm'
            # Add more mappings as needed
        }
        try:
            for file in download_batch:
                folder_name = file['year'] + "/" + file['month']
                response = requests.get(file['media_url'])
                file_extension = ''
                if 'content-type' in response.headers:
                    # Ensure lowercase for consistency
                    content_type = response.headers['content-type'].lower()
                    file_extension = content_type_to_extension.get(
                        content_type, '.unknown')
                else:
                    file_extension = '.unknown'
                if response.status_code == 200:
                    # Extract the file name from the URI
                    file_name = os.path.basename(
                        file['file_name']+file_extension)

                    # Create the full path to save the file
                    file_path = os.path.join(
                        self.check_make_dir(folder_name), file_name)

                    with open(file_path, 'wb') as file:
                        file.write(response.content)
                else:
                    print(
                        f"Failed to download: {file['file_name']+file_extension}")
        except Exception as e:
            print(f"Error downloading: {str(e)}")

    # Function to create and update the progress bar
    def update_progress(iteration, total, bar_length=50):
        progress = float(iteration)/total
        arrow = '=' * int(round(bar_length * progress))
        spaces = ' ' * (bar_length - len(arrow))
        sys.stdout.write(f'\r[{arrow + spaces}] {int(progress * 100)}%')
        sys.stdout.flush()

    def create_conversation_job(self, intervals):
        body = PureCloudPlatformClientV2.AsyncConversationQuery()
        body.interval = intervals
        body.order = 'asc'
        try:
            api_response = self.conversation_api_instance.post_analytics_conversations_details_jobs(
                body)
            return api_response._job_id
        except ApiException as analytics_excep:
            self.log_exception_to_file(
                analytics_excep, "Exception when calling AnalyticsApi->post_analytics_conversations_details_jobs:"+analytics_excep.reason)

    def wait_for_job_completion(self, job_id):
        while True:
            try:
                job_status = self.conversation_api_instance.get_analytics_conversations_details_job(
                    job_id)
                status = job_status.state
                if status == 'FULFILLED':
                    print("Job is completed")
                    break
                elif status in ['QUEUED', 'PENDING']:
                    print(
                        f"Job status: {status}. Checking again in 5 seconds...")
                    time.sleep(5)
                elif status in ['FAILED', 'CANCELLED', 'EXPIRED']:
                    print(f"Job is {status.lower()}")
                    break
                else:
                    print(
                        f"Job status: {status}. Checking again in 5 seconds...")
                    time.sleep(5)
            except ApiException as conversion_exep:
                self.log_exception_to_file(
                    conversion_exep, "Exception when calling AnalyticsApi->post_analytics_conversations_details_jobs: " + conversion_exep.reason)

    def get_conversation_details(self, conversation_id):
        try:
            recording_meta = self.recording_instance.get_conversation_recordingmetadata(
                conversation_id)
            for meta in recording_meta:
                try:
                    recording = self.recording_instance.get_conversation_recording(
                        conversation_id, meta.id)
                    return recording
                except ApiException as conversion_exep:
                    self.log_exception_to_file(
                        conversion_exep, "Exception when calling recording_instance->get_conversation_recording " + conversion_exep.reason)
        except ApiException as conversion_exep:
            self.log_exception_to_file(
                conversion_exep, "Exception when calling recording_instance->get_conversation_recordingmetadata: " + conversion_exep.reason)

    def extract_conversation_details(self, job_id, intervals, csv_file_path, csv_file_path2):
        print("Processing... Please wait...")
        try:
            job_details = self.analytics_api_instance.get_analytics_conversations_details_job_results(job_id)
            with open(csv_file_path, 'w', newline='') as csvfile:
                fieldnames = ['Conversation Id', 'Agent Name', 'Start Date', 'End Date', 'ANI', 'Purpose', 'Media Files']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                # Write the header row to the CSV file
                writer.writeheader()

                noConversations = 0
                noRecordings = 0
                noFailedConversations = 0
                failedConversations = []
                # Loop all conversations in the Job
                for item in job_details.conversations:
                    if item.conversation_id:
                        # media_type = ''
                        ani = ''
                        customer_direction = ''
                        for participant in item.participants:
                            for session in participant.sessions:
                                # media_type = session.media_type
                                ani = session.ani
                                customer_direction = session.direction
                        noConversations += 1
                        # conversation_ids.append(item.conversation_id)
                        try:
                            recording = self.get_conversation_details(
                                item.conversation_id)
                            concatenated_agents = ''
                            concatenated_file_names = ''
                            try:
                                for agent in recording.users:
                                    concatenated_agents += agent.name + '/'
                                # Remove the trailing '/' if it exists
                                if concatenated_agents.endswith('/'):
                                    concatenated_agents = concatenated_agents[:-1]
                            except Exception as ex:
                                self.log_exception_to_file(
                                    ex, "Exception while processing users for Conversation "+item.conversation_id)
                                continue  # Skip this conversation and continue with the next one
                            details_wrapper = DetailsWrapper(concatenated_agents, recording.start_time, recording.end_time, ani, customer_direction, concatenated_file_names)
                            print(details_wrapper)
                            writer.writerow({
                                'Conversation Id': item.conversation_id,
                                'Agent Name': concatenated_agents,
                                'Start Date': self.change_timezone(str(item.conversation_start), 'UTC', 'Australia/Sydney'),
                                'End Date': self.change_timezone(str(item.conversation_end), 'UTC', 'Australia/Sydney'),
                                'ANI': ani,
                                'Purpose': customer_direction,
                                'Media Files': concatenated_file_names
                            })
                        except ApiException as ex:
                            noFailedConversations += 1
                            failedConversations.append(
                                item.conversation_id)
                            self.log_exception_to_file(
                                ex, "Exception while processing call recordings for Conversation "+item.conversation_id+": " + ex.reason)
                            continue  # Skip this conversation and continue with the next one

                print("\nProcessing completed.")
                print("Total number of conversations:%s\n" % noConversations)
                print("Total number of recordings:%s\n" % noRecordings)
                print("Total number of failed conversations:%s\n" %
                      len(failedConversations))

        except ApiException as analytics_excep:
            self.log_exception_to_file(
                analytics_excep, "Exception while AnalyticsApi->get_analytics_conversations_details_job_results:" + analytics_excep.reason)


class DetailsWrapper:
    def __init__(self, user_name, start_time, end_time, ani, direction, url):
        self.user_name = user_name
        self.start_time = start_time
        self.end_time = end_time
        self.ani = ani
        self.direction = direction
        self.url = url

    def to_dict(self):
        return {
            'user_name': self.user_name,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'ani': self.ani,
            'direction': self.direction,
            'url': self.url
        }

    def __str__(self):
        return f'DetailsWrapper({self.user_name}, {self.start_time}, {self.end_time}, {self.ani}, {self.direction}, {self.url})'


# Constants
CLIENT_ID = ''
CLIENT_SECRET = ''
ORG_REGION = ''  # eg. us_east_1
intervals = "2019-01-01T00:00:00/2023-09-14T00:00:00"
csv_file_path = 'meta_data.csv'
csv_file_path2 = 'failed_output.csv'

# Main program
if __name__ == "__main__":
    oauth_manager = OAuthManager(CLIENT_ID, CLIENT_SECRET)
    api_client = oauth_manager.get_api_client()

    extractor = ConversationDetailsExtractor(api_client)

    job_id = extractor.create_conversation_job(intervals)
    extractor.wait_for_job_completion(job_id)
    print(job_id)
    extractor.extract_conversation_details(job_id, intervals, csv_file_path, csv_file_path2)
