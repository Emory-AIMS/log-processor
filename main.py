import json
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import boto3

import config
import s3_manager
import transformer

# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#queue

BASE_DIR_TRANSFORMER = "tmp-covapp"
LOCAL_PATH = config.get_base_dir()
BASE_DIR_S3 = LOCAL_PATH + "/" + BASE_DIR_TRANSFORMER


def polling_queue():
    print("polling queue", LOCAL_PATH)
    sqs = boto3.resource("sqs", region_name='us-west-1')

    queue_read = sqs.Queue(config.get_sqs_url_covapp())

    while 1:
        print("asking for new messages")
        messages = queue_read.receive_messages(WaitTimeSeconds=5)
        print("Messages found:", len(messages))
        for message in messages:
            print("Message received: {0}".format(message.body))

            try:
                body = json.loads(message.body)
                elaborate_message(body)
                message.delete()
                # todo: move to a backup bucket

            except Exception as e:
                print("EXCEPTION ARISE", e)
            print("finished")


def sync_files():
    year = str(datetime.now().year)

    copying_folder = "{}/{}".format(BASE_DIR_S3, year)

    if os.path.exists(copying_folder) and os.path.isdir(copying_folder):
        print("no files to sync, skipping")

    script = "aws s3 sync {}/{} s3://{}/{}".format(
        BASE_DIR_S3, year, config.get_s3_bucket(), year
    )
    process = subprocess.Popen(script.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    print("OUTPUT", output[:200], '...blablabla')
    print("ERROR", error)


def delete_tmp_folder():
    dirpath = os.path.join(os. getcwd(), LOCAL_PATH)
    # dirpath = os.path.join(LOCAL_PATH, BASE_DIR_S3)
    if os.path.exists(dirpath) and os.path.isdir(dirpath):
        print("deleting temp folder")
        shutil.rmtree(dirpath)


def create_tmp_folder():
    try:
        Path(BASE_DIR_S3).mkdir(parents=True, exist_ok=True)
    except OSError:
        print("Creation of the directory %s failed" % BASE_DIR_S3)
        raise OSError
    else:
        print("Successfully created the directory %s " % BASE_DIR_S3)


def elaborate_message(body):
    delete_tmp_folder()
    create_tmp_folder()

    if "Records" in body:
        record = dict(body['Records'][0])
        arn = record['s3']['bucket']['arn'] + '/' 
        file_name = arn + record['s3']['object']['key']
        local_path = s3_manager.download_file(file_name, BASE_DIR_S3)
        if local_path is not None:
            transformer.run(local_path, BASE_DIR_S3)
            sync_files()
            s3_manager.move_file(file_name)

    delete_tmp_folder()


def test():
    body = {
        "eventTime": "2020-04-01T21:20:30Z",
        "ARN": "arn:aws:s3:::s3-temporary-interaction-log/postdata.log.1",
    }
    elaborate_message(body)


if __name__ == "__main__":

    if len(sys.argv) > 1:
        # /media/covid-tmp
        LOCAL_PATH = sys.argv[1]
        BASE_DIR_S3 = LOCAL_PATH + "/" + BASE_DIR_TRANSFORMER

    polling_queue()
    # test()
