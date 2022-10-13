from datetime import datetime
import os
from google.cloud import storage_transfer
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\Admin\PycharmProjects\pythonProject\eclub-data-d18845fd301c.json'

def create_one_time_aws_transfer(
        project_id: str, description: str,
        source_bucket: str, aws_access_key_id: str,
        aws_secret_access_key: str, sink_bucket: str):

    client = storage_transfer.StorageTransferServiceClient()

    now = datetime.utcnow()
    # Setting the start date and the end date as
    # the same time creates a one-time transfer
    one_time_schedule = {
        'day': now.day,
        'month': now.month,
        'year': now.year
    }

    transfer_job_request = storage_transfer.CreateTransferJobRequest({
        'transfer_job': {
            'project_id': project_id,
            'description': description,
            'status': storage_transfer.TransferJob.Status.ENABLED,
            'schedule': {
                'schedule_start_date': one_time_schedule,
                'schedule_end_date': one_time_schedule
            },
            'transfer_spec': {
                'aws_s3_data_source': {
                    'bucket_name': source_bucket,
                    'aws_access_key': {
                        'access_key_id': aws_access_key_id,
                        'secret_access_key': aws_secret_access_key,
                    }
                },
                'gcs_data_sink': {
                    'bucket_name': sink_bucket,
                }
            }
        }
    })

    result = client.create_transfer_job(transfer_job_request)
    print(f'Created transferJob: {result.name}')

if __name__ == "__main__":
    create_one_time_aws_transfer(project_id = 'eclub-data',description = 'Transfer job for Automating workflow from AWS S3 to GCS',
                                 source_bucket = 'evergreenclub-data-engineering-prod',aws_access_key_id = 'AKIAUWDJH4NZDMGK7J7Z',
                                 aws_secret_access_key = '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',sink_bucket = 'eclub_data_prod_product')

