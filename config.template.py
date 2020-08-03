##################
# AWS Parameters #
##################

# AWS SQS queue url where log files path are stored
SQS_QUE_URL_COVAPP = 'XXXXXXXXXX'
# AWS S3 bucket name where interactions will be stored
BUCKET_S3_NAME = 'XXXXXXXXXX'
# AWS S3 bucket name where parsed log files will be stored before deletion
BUCKET_S3_COPY_NAME = 'XXXXXXXXXX'
# base directory where temprary files will be stored 
BASE_DIR = "/tmp-working"


def get_sqs_url_covapp():
    return SQS_QUE_URL_COVAPP


def get_s3_bucket():
    return BUCKET_S3_NAME


def get_base_dir():
    return BASE_DIR


def get_s3_copy_bucket():
    return BUCKET_S3_COPY_NAME
