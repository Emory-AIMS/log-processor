
import boto3
import config

s3 = boto3.resource("s3")


def extract_bucket_key(arn_path):
    s3_path = arn_path.replace("arn:aws:s3:::", "")
    splitted = s3_path.split("/")
    bucket = splitted[0]
    key = "/".join(splitted[1:])
    return bucket, key


# 'arn:aws:s3:::s3-temporary-interaction-log/postdata.log.1'
def download_file(arn_path, local_path):

    bucket, key = extract_bucket_key(arn_path)

    try:
        s3.Bucket(bucket).download_file(key, local_path + "/" + key)
        return local_path + "/" + key
    except Exception as e:
        print(e)
    return None


def move_file(arn_path):
    bucket_source_name, key = extract_bucket_key(arn_path)
    copy_source = {
        'Bucket': bucket_source_name,
        'Key': key
    }
    try:
        bucket_source = s3.Bucket(bucket_source_name)
        bucket_copy = s3.Bucket(config.get_s3_copy_bucket())
        bucket_copy.copy(copy_source, key)
        response = bucket_source.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key
                    },
                ]
            }
        )
        print('RESPONSE MOVE', response)
    except Exception as e:
        print(e)
    return None


if __name__ == "__main__":
    # a = download_file(
    #     "arn:aws:s3:::s3-temporary-interaction-log/postdata.log.1",
    #     "/Users/antonioromano/development/coronavirus-project/consumer-covapp",
    # )
    # print(a)
    arn_path = "arn:aws:s3:::s3-temporary-interaction-log/postdata.log.1"
    move_file(arn_path)

