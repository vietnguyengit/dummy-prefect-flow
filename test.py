import boto3
from prefect import flow, task, get_run_logger


LANDING_BUCKET = "vhnguyen-prefect-landing"
RAW_BUCKET = "vhnguyen-prefect-raw"
s3 = boto3.resource("s3")


def is_bucket_empty(bucket):
    for obj in bucket.objects.all():
        if obj is not None:
            return False
    return True


@task
def move_objects(src_bucket, dest_bucket):
    for obj in src_bucket.objects.all():
        copy_source = {
            'Bucket': LANDING_BUCKET,
            'Key': obj.key
        }
        dest_bucket.copy(copy_source, obj.key)
        s3.Object(LANDING_BUCKET, obj.key).delete()


@flow
def dummy_landing_to_raw():
    logger = get_run_logger()
    landing_bucket = s3.Bucket(LANDING_BUCKET)
    raw_bucket = s3.Bucket(RAW_BUCKET)
    if not is_bucket_empty(landing_bucket):
        logger.info("running fake files processor")
        logger.info("moving landing files to raw")
        move_objects(landing_bucket, raw_bucket)
        logger.info("deleted landing files")
    else:
        logger.info("landing bucket is empty")
        return


if __name__ == "__main__":
    dummy_landing_to_raw()
