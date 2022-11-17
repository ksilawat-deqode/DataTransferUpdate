import json
import os
from time import sleep

import boto3
import psycopg2
from psycopg2.extras import RealDictCursor

import logging
from datetime import datetime
from pythonjsonlogger import jsonlogger


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(
            log_record,
            record,
            message_dict
        )
        if not log_record.get('timestamp'):
            now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            log_record['time'] = now
        if log_record.get('level'):
            log_record['level'] = log_record['level'].lower()
        else:
            log_record['level'] = record.levelname.lower()
        log_record['source'] = "DataTransferUpdate"


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = CustomJsonFormatter('%(level)s %(msg)s %(time)s %(source)s')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


def lambda_handler(event, context):
    connection = psycopg2.connect(
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD"),
        host=os.environ.get("DB_HOST"),
        port=os.environ.get("DB_PORT"),
        database=os.environ.get("DB_NAME"),
    )

    cursor = connection.cursor(cursor_factory=RealDictCursor)

    select_query = "SELECT * FROM emr_job_details WHERE id=%s"
    cursor.execute(select_query, (event["id"], ))
    data = cursor.fetchone()

    secrets_client = boto3.client("secretsmanager")
    polling_function_arn = json.loads(
        secrets_client.get_secret_value(
            SecretId=os.environ.get("SECRETS"),
        ).get("SecretString")
    ).get("POLLING_FUNCTION_ARN")

    data_sync_client = \
        boto3.client("datasync", region_name=event["region_name"])
    status = data_sync_client.describe_task_execution(
        TaskExecutionArn=event["task_execution_arn"],
    ).get("Status")

    data_transfer_state_update_query = "UPDATE emr_job_details SET " \
                                       "data_transfer_state=%s WHERE id=%s"

    cursor.execute(
        data_transfer_state_update_query,
        (status, event["id"])
    )

    extra_log_info = {
        "clientIp": data.get("client_ip"),
        "destinationBucket": data.get("destination"),
        "id": data.get("id"),
        "jti": data.get("jti"),
        "query": data.get("query"),
        "region": data.get("cross_bucket_region"),
        "skyflowRequestId": data.get("requestId"),
    }

    logger.info(
        f"Polling jobstatus: {status}",
        extra=extra_log_info,
        )
    if status in ("SUCCESS", "ERROR"):
        status = status if status != "ERROR" else "FAILED"

        logger.info(
            f"Updating jobstatus: {status}",
            extra=extra_log_info,
            )
        update_query = "UPDATE emr_job_details SET jobstatus=%s WHERE id=%s"
        cursor.execute(
            update_query,
            (status, event["id"])
        )
        connection.commit()
        cursor.close()
        return

    sleep_interval = int(os.environ.get("POLLING_INTERVAL"))

    logger.info(
        f"Sleeping for {sleep_interval} seconds",
        extra=extra_log_info,
        )
    sleep(sleep_interval)

    payload = {
        "id": event["id"],
        "region_name": event["region_name"],
        "task_execution_arn": event["task_execution_arn"],
    }
    payload_bytes = json.dumps(payload).encode('utf-8')

    logger.info(
        f"Performing recursive invocation",
        extra=extra_log_info,
        )

    boto3.client("lambda").invoke_async(
        FunctionName=polling_function_arn,
        InvokeArgs=payload_bytes,
    )
