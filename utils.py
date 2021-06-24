from __future__ import print_function  # Python 2/3 compatibility
import json
import uuid
import boto3
import datetime

import os
import logging
from boto3.dynamodb.conditions import Key, Attr

sts_client = boto3.client('sts', region_name='us-east-1')  ## PROD
# sts_client = boto3.client('sts', region_name='ap-south-1')  ##DEV


def myconverter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()


def upload_file_to_s3(bucket_name, filename, data):
    s3_resource = boto3.resource('s3')
    s3_object = s3_resource.Object(bucket_name, filename)
    s3_object.put(Body=json.dumps(data, default=myconverter))


def create_file_name(tenant_id, account_id, resource_type):
    return tenant_id + "_" + account_id + "_" + resource_type + "_" + str(uuid.uuid5(uuid.NAMESPACE_DNS,
                                                                          str(datetime.datetime.today()))) + ".txt"


def save_pointer_to_s3(tenant_id, account_id, resource_type, filename):
    final_save_resp = {
        'client_id': tenant_id,
        'account_id': account_id,
        'resource_type': resource_type,
        'pointer': filename
    }
    return final_save_resp



# Set Logging Level
logger = logging.getLogger()
logger.setLevel(int(os.getenv("log_level", 10)))
#
dynamodb_client = boto3.resource("dynamodb", region_name='us-east-1')  #PROD
ssm_client = boto3.client("ssm", region_name='us-east-1') #PROD
#
# dynamodb_client = boto3.resource("dynamodb", region_name='ap-south-1')  #DEV
# ssm_client = boto3.client("ssm", region_name='ap-south-1') #DEV

def get_credential():
    # ssm_strings = [os.getenv("credentials_path", "/dev/pb/credentials")]  # DEV
    ssm_strings = [os.getenv("credentials_path", "/prod/pb/credentials")]   # PROD
    print("Validating SSM Parameters for: {}".format(ssm_strings))
    response = ssm_client.get_parameters(
        Names=ssm_strings,
        WithDecryption=True
    )
    p = response['Parameters'][0]
    t = p['Value'].replace("'", "\"")
    d = json.loads(t)
    return d


def get_ssm(ssm_strings):
    response = ssm_client.get_parameters(
            Names=ssm_strings,
            WithDecryption=True
    )
    logger.info(response)
    aws_credentials(response)
    return aws_credentials(response)


def aws_credentials(response):
    res = []
    for i in response['Parameters']:
        data = (i['Value'].replace("'", "\""))
        d = json.loads(data)
        message = {'role_arn': d['role_arn'], 'external_id': d['external_id'], 'tenant_id': i['Name'].split('/')[4],
                "account_id": (d['role_arn']).split(':')[4]}
        res.append(message)
    return res


def mysql_response():
    parameters = []
    try:
        import requests
        import json
        user_data = get_credential()
        # url = os.getenv('loginapiurl', "https://apibackend.dev.powerboard.in/api/v1/auth/users/login/")  # DEV
        url = os.getenv('loginapiurl', "https://authapi.app.loves.cloud/api/v1/auth/users/login/")  # PROD
        data = {
            "user": user_data
        }
        payload = json.dumps(data)
        headers = {
            'Content-Type': "application/json"
        }
        return_res = requests.post(url, data=payload, headers=headers)
        # url = os.getenv('cloudactiveapiurl',
        #                 "https://apibackend.dev.powerboard.in/api/v1/organization/adminaccesscloudproviderlist/")  ## DEV
        url = os.getenv('cloudactiveapiurl',
                        "https://authapi.app.loves.cloud/api/v1/organization/adminaccesscloudproviderlist/")  ## PROD
        headers = {
            'Authorization': 'Token ' + str(return_res.json()['user']['token'])
        }
        temp = requests.get(url, headers=headers)

    except Exception as e:
        print("ERROR: TYPE# P1 - Unable to start scheduling. Transction ID: {}")
        logger.error("ERROR: TYPE# P1 - : Unable to start schedulingTransction ID: {}")
        return {"statusCode": 500, "status": "ERROR", "TYPE": "P1", "message": "Unable to start scheduling",
                "txn_id": str(e)}
    try:
        ctr = 0
        for t in temp.json():
            if 'aws' in t['param_store_path']:
                parameters.append(t['param_store_path'])
                organization_id = t['param_store_path'].split('/')[4]
        logger.info("Publishing Message for {c} - {i}".format(i=organization_id, c=ctr))
        ctr = ctr + 1
        logger.info("SUCCESS: TYPE# OK - : Finished scheduling Transction ID: {}")
    except Exception as e:
        logger.error("Erorr while scheduling Transction ID: {}")
    return get_ssm(parameters)
