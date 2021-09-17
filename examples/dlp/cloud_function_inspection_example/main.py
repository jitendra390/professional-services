from google.cloud import dlp
from google.cloud import storage
from google.cloud import securitycenter
import os

PROJECT_ID = os.environ.get('PROJECT_ID', 'Specified environment variable is not set.')
INSPECT_TEMPLATE = os.environ.get('INSPECT_TEMPLATE' , 'Specified environment variable is not set.')


# Initialize the Google Cloud client libraries
dlp = dlp.DlpServiceClient()
storage_client = storage.Client()
client = securitycenter.SecurityCenterClient()

#Function to create DLP job
def create_DLP_job(data,done):
  """This function is triggered by new files uploaded to the designated Cloud Storage quarantine/staging bucket.

       It creates a dlp job for the uploaded file.
    Arg:
       data: The Cloud Storage Event
    Returns:
        None. Debug information is printed to the log.
    """
  # Get the targeted file in the quarantine bucket
  file_name = data['name']
  bucket_name = data['bucket']
  print('Function triggered for file [{}]'.format(file_name))

  

  # Convert the project id into a full resource id.
  parent ="projects/{}/locations/us".format(PROJECT_ID)

  inspect_job = {
       'inspect_template_name': INSPECT_TEMPLATE,
      
       'storage_config': {
          'cloud_storage_options': {
              'file_set': {
                  'url':
                      'gs://{bucket_name}/{file_name}'.format(
                          bucket_name=bucket_name, file_name=file_name)
              }
          }
      },
      'actions': [{
          "publish_summary_to_cscc":{},
          "save_findings":{
              "output_config":{
                "table":{
                  "project_id":"project",
                  "dataset_id":"dataset",
                  "table_id":""
                }
              }
          }
      }]
  }

  # Create the DLP job and let the DLP api processes it.
  try:
    dlp.create_dlp_job(parent=(parent), inspect_job=(inspect_job))
    print('Job created by create_DLP_job')
  except Exception as e:
    print(e)
