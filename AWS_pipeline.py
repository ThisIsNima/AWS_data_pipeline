import boto3

# Step 1: Create an S3 Bucket
def create_s3_bucket(bucket_name):
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket=bucket_name)

# Step 2: Define an AWS Glue ETL Job
def create_glue_job(job_name, script_location, role_arn, source_path, target_path):
    glue = boto3.client('glue')

    response = glue.create_job(
        Name=job_name,
        Role=role_arn,
        Command={'Name': 'glueetl',
                 'ScriptLocation': script_location},
        DefaultArguments={
            '--source_path': source_path,
            '--target_path': target_path
        }
    )

# Step 3: Trigger the AWS Glue Job using Lambda
def trigger_glue_job(job_name):
    client = boto3.client('glue')

    response = client.start_job_run(
        JobName=job_name,
        MaxCapacity=2  # Adjust as needed
    )

# Example Usage
if __name__ == "__main__":
    bucket_name = "your-s3-bucket-name"
    create_s3_bucket(bucket_name)

    job_name = "your-glue-job-name"
    script_location = "s3://your-bucket-name/your-glue-script.py"
    role_arn = "arn:aws:iam::your-account-id:role/your-role-name"
    source_path = "s3://your-bucket-name/source-data/"
    target_path = "s3://your-bucket-name/target-data/"

    create_glue_job(job_name, script_location, role_arn, source_path, target_path)

    trigger_glue_job(job_name)
