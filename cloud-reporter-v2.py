import os
import json
import glob
import uuid
import time
import utils
import boto3
import datetime
from maps import message_map, message_list, key_map, service_dic, action_list, action_map

now = datetime.datetime.now()
dt_string = now.strftime("%m-%d-%Y-%H")
# creds = utils.mysql_response()
creds = [
  {
    'role_arn': 'arn:aws:iam::354265195620:role/Powerboard-Stack-lcRoleArn-27KPBLGBK9RN',
    'external_id': 'lovescloud-PS0BfXsi0d',
    'tenant_id': '562d2d19-41ea-4387-a2ad-ed284ecccfab',
    'account_id': '354265195620'
  }
]

# 
# prod_cloud_reports_data

RAW_UPLOAD_PATH = os.getenv("raw_upload_path", "raw")
if os.getenv("bucket_name") is not None:
    bucket_name = os.getenv("bucket_name")
else:
    # bucket_name = input("Enter the S3 bucket name where the reports needs to be saved: ")
    bucket_name = 'prod-pb-cloud-reports'
POINTER_UPLOAD_PATH = os.getenv('pointer_upload_path', "pointer")
cloud_provider = "AWS"
if os.getenv("table_name") is not None:
    table_name = os.getenv("table_name")
else:
    # table_name = input("Enter the DynamoDB table name where the pointers needs to be saved: ")
    table_name = 'prod_cloud_reports_data'
dynamo_db = boto3.resource('dynamodb', region_name='us-east-1') # PROD
# dynamo_db = boto3.resource('dynamodb', region_name='ap-south-1') # DEV
table = dynamo_db.Table(table_name)


def save_data_to_ddb(client_id: str, account_id: str, response: object, resource_type: str,
                     cloud_provider_resource_group: str, cloud_provider_resource_type: str, resource_sub_type: str):
    transaction_id = uuid.uuid5(uuid.NAMESPACE_DNS, str(uuid.uuid4()))
    response = table.put_item(
        Item={
            'cloud-provider': cloud_provider,
            'cloud_provider_resource_group': cloud_provider_resource_group,
            'cloud_provider_resource_type': cloud_provider_resource_type,
            'client_id': client_id+'_'+account_id,
            'generated_id': client_id+'_'+account_id+'-'+str(transaction_id),
            'resource_sub_type': resource_sub_type,
            'resource_type': resource_type,
            'timestamp': int(time.time()),
            'date': datetime.datetime.today().isoformat(),
            'result': response,
        }
    )
    return response


def publish_reports(data, tenant_id, account_id):
    client_id = tenant_id
    try:
        checks = []
        for service in service_dic():
            good = 0
            failure = 0
            warning = 0
            resp = {account_id: {service: data[service]}}
            sub_checks = []
            for key, value in resp[account_id].items():
                for k, v in (resp[account_id][key].items()):
                    sub_good = 0
                    sub_failure = 0
                    sub_warning = 0
                    response = {k: resp[account_id][key][k]}
                    for sev in response:
                        result = []
                        final_response = {'response': result}
                        for region in response[sev].get('regions'):
                            for resources in response[sev].get('regions')[region]:
                                if 'action' not in resources:
                                    resources['action'] = "No action required"
                                for resources_key, resources_value in resources.items():
                                    if resources_value == 'Good':
                                        sub_good += 1
                                        good += 1

                                    if resources_value == 'Failure':
                                        sub_failure += 1
                                        failure += 1

                                    if resources_value == 'Warning':
                                        sub_warning += 1
                                        warning += 1

                                    if resources_key == 'message':
                                        if resources_value in message_list():
                                            message = message_map()[resources_value]
                                        else:
                                            message = resources_value

                                    if resources_key == 'action':
                                        if resources_value in action_list():
                                            actions = action_map()[resources_value]
                                        else:
                                            actions = resources_value

                                        result.append({'region': region, 'resourceSummary': resources['resourceSummary'],
                                                     'message': message, 'action': actions,
                                                     'severity': resources['severity']})

                        resource_type = k
                        filename = utils.create_file_name(tenant_id, account_id, resource_type)
                        utils.upload_file_to_s3(bucket_name, '/'.join([tenant_id, POINTER_UPLOAD_PATH, filename]), final_response)
                        final_save_resp = utils.save_pointer_to_s3(tenant_id, account_id, resource_type,
                                                                   '/'.join([tenant_id,
                                                                             POINTER_UPLOAD_PATH,
                                                                             filename]))
                        save_data_to_ddb(client_id, account_id, final_save_resp, resource_type=key.replace("aws.", "", 1),
                                         cloud_provider_resource_group=key,
                                         cloud_provider_resource_type=key + "/" + k, resource_sub_type=k)
                    sub_checks.append({"name": k, "results": {"good": sub_good, "failure": sub_failure,
                                                              "warning": sub_warning}})
                checks.append({"name": key_map()[key], "results": {"good": good, "failure": failure, "warning": warning}})
            sub_checks_response = {"response": sub_checks}
            resource_type_sub_checks = key.replace("aws.", "", 1) + "_" + "sub_checks"
            filename_sub_checks = utils.create_file_name(tenant_id, account_id, resource_type_sub_checks)
            final_save_resp_sub_checks = utils.save_pointer_to_s3(tenant_id, account_id, resource_type_sub_checks, '/'
                                                                  .join([tenant_id, POINTER_UPLOAD_PATH,
                                                                         filename_sub_checks]))
            utils.upload_file_to_s3(bucket_name, '/'.join([tenant_id, POINTER_UPLOAD_PATH, filename_sub_checks]),
                                    sub_checks_response)
            save_data_to_ddb(client_id, account_id, final_save_resp_sub_checks, resource_type=key.replace("aws.", "", 1),
                             cloud_provider_resource_group=key, cloud_provider_resource_type=key + "/" + k,
                             resource_sub_type=resource_type_sub_checks)
        checks_response = {"response": checks}
        resource_type_checks = "checks"
        filename_checks = utils.create_file_name(tenant_id, account_id, resource_type_checks)
        utils.upload_file_to_s3(bucket_name, '/'.join([tenant_id, POINTER_UPLOAD_PATH, filename_checks]),
                                checks_response)
        final_save_resp_checks = utils.save_pointer_to_s3(tenant_id, account_id, resource_type_checks,
                                                   '/'.join([tenant_id,
                                                             POINTER_UPLOAD_PATH,
                                                             filename_checks]))
        save_data_to_ddb(client_id, account_id, final_save_resp_checks, resource_type="aws",
                         cloud_provider_resource_group="checks", cloud_provider_resource_type="checks",
                         resource_sub_type="checks")
    except Exception as e:
        print('exception %s against account_id: %s' % (e, account_id))


def security_report():
    for i in range(len(creds)):
        tenant_id = creds[i]['tenant_id']
        print("running against........", tenant_id)
        try:
            print("assuming role against........", tenant_id)
            response = utils.sts_client.assume_role(
                RoleArn=creds[i]['role_arn'],
                RoleSessionName='AssumeRoleSession1',
                ExternalId=creds[i]['external_id']
            )

            accesskey_id = response['Credentials']['AccessKeyId']
            secretaccess_key = response['Credentials']['SecretAccessKey']
            session_token = response['Credentials']['SessionToken']
            os.environ["AWS_ACCESS_KEY_ID"] = accesskey_id
            os.environ["AWS_SECRET_ACCESS_KEY"] = secretaccess_key
            os.environ["AWS_SESSION_TOKEN"] = session_token
            os.system(f"export AWS_ACCESS_KEY_ID={accesskey_id} AWS_SECRET_ACCESS_KEY={secretaccess_key} "
                      f"AWS_SESSION_TOKEN={session_token}")
            account_id = json.loads(os.popen('aws sts get-caller-identity').read())['Account']
            os.system('aws sts get-caller-identity')
            print("running cloud reports against tenant_id and account_id........", tenant_id, account_id)
            os.system("npm run scan -- -f json")
            os.chdir(os.getcwd())
            for file in glob.glob(f"scan_report_*.json"):
                os.rename(file, f"scan_report_{account_id}.json")
            for new_file in glob.glob(f"scan_report_{account_id}.json"):
                with open(new_file) as json_data:
                    try:
                        data = json.load(json_data)
                        print("Uploading raw data against ........", tenant_id)
                        utils.upload_file_to_s3(bucket_name, '/'.join([tenant_id, RAW_UPLOAD_PATH, new_file]), data)
                        print("now bifurcating data against ........", tenant_id)
                        publish_reports(data, tenant_id, account_id)
                    except Exception as e:
                        print('exception %s against account_id: %s' % (e, account_id))
                        continue
            os.system('unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN')
        except Exception as e:
            print('exception %s against tenant_id: %s' % (e, tenant_id))
            continue


security_report()


# prod-pb-cloud-reports
# prod_cloud_reports_data
#
# dev-pb-cloud-reports
# dev_cloud_reports_data