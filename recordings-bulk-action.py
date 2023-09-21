import os
import sys
import time
import PureCloudPlatformClientV2
from PureCloudPlatformClientV2.rest import ApiException

print('-------------------------------------------------------------')
print('- Execute Bulk Action on recordings-')
print('-------------------------------------------------------------')

# Credentials
CLIENT_ID = ''
CLIENT_SECRET = ''
ORG_REGION = ''  # eg. us_east_1

# Set environment
region = PureCloudPlatformClientV2.PureCloudRegionHosts[ORG_REGION]
PureCloudPlatformClientV2.configuration.host = region.get_api_host()

# OAuth when using Client Credentials
api_client = PureCloudPlatformClientV2.ApiClient(
).get_client_credentials_token(CLIENT_ID, CLIENT_SECRET)

try:
    # Get the api
    conversations_api = PureCloudPlatformClientV2.ConversationsApi(api_client)
    recordings_api = PureCloudPlatformClientV2.RecordingApi(api_client)
    body = PureCloudPlatformClientV2.ConversationQuery()
    body.interval = "2017-01-01T00:00:00/2021-12-31T00:00:00"
    # body.order = 'asc'
    batchRequestBody = PureCloudPlatformClientV2.BatchDownloadJobSubmission()
    bacthDownloadRequest = PureCloudPlatformClientV2.BatchDownloadJobResult()
    
    try:
        conversations_details = conversations_api.post_analytics_conversations_details_jobs(body)
        print(conversations_details.job_id)
        try:
            job_results = conversations_api.get_analytics_conversations_details_job_results(conversations_details.job_id)
            print(len(job_results.conversations))
            for conversation in job_results.conversations:
                print(conversation.conversation_id)
                conversation_meta = recordings_api.get_conversation_recordingmetadata(conversation.conversation_id)
                for meta in conversation_meta:
                    print(meta)
                    bacthDownloadRequest = PureCloudPlatformClientV2.BatchDownloadJobResult()
                    # bacthDownloadRequest.conversation_id = meta.conversation_id
                    # bacthDownloadRequest.conversation_id = meta.id
                    # batchRequestBody.batch_download_request_list.append(bacthDownloadRequest)
                    # post_batch  = recordings_api.post_recording_batchrequests(batchRequestBody)
                    # get_recording = recordings_api.get_recording_batchrequest(post_batch.id)
        except ApiException as e:
            print(f"Exception when calling conversations_api->get_analytics_conversations_details_job_results: { e }")
    except ApiException as e:
        print(f"Exception when calling conversations_api->post_analytics_conversations_details_jobs: { e }")
        sys.exit()

except ApiException as e:
    print(f"Exception when calling RecordingApi->post_recording_jobs: { e }")
    sys.exit()
