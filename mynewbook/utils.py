import json
import os
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

import boto3
import botocore
import botocore.session
import requests
from boto3.session import Session
from botocore import credentials
from botocore.credentials import CredentialResolver, SSOProvider

AWS_PROFILE = os.environ["AWS_PROFILE"]
DATA_PROCESSING_INGEST_PREFIX = "main-data-processing-ingest"
DATA_PROCESSING_OUTPUT_PREFIX = "main-data-processing-output"
SUBSETTING_DATA_PROCESSING_INGEST_PREFIX= "main-adssubsettingdp-data-processing-ingest"
ENGINE_INGEST_PREFIX = "main-engine-ingest"
ACCEPTED_NUMBER_OF_BUCKETS = 1
API_KEY_PARAMETER: str = "/metoffice/shc/main/main-shc/service_hub_api_key"
API_URL: str = "https://lotus.hub.metoffice.cloud/v1/datasets"
THIRTY_MINUTES = 30 * 60
REGION = os.environ.get("AWS_REGION", "eu-west-2")


def get_session(role_arn: Optional[str] = None, profile_name: Optional[str] = AWS_PROFILE) -> Session:
    """
    Get a session that's configured to use the same credentials cache as the AWS CLI (rather than the
    separate cache used by boto3 by default). This means the CLI and python scripts can share cached MFA-protected
    credentials and python scripts won't have to reauthenticate every time they run.
    role_arn and profile_name can be combined, in which case the named profile is used to create the session and then
    the specified role is assumed.
    use_sso controls whether or not to authenticate using AWS Single Sign-On. The default behaviour is not to use SSO,
    and use AWS IAM based authentication.

    :param role_arn: The ARN of the IAM role to assume.
    :param profile_name: The name of a boto3 credentials profile to use when accessing AWS
    :return: a boto3.Session configured to use cached MFA credentials
    """
    botocore_session = botocore.session.Session(profile=profile_name)
    credential_provider: CredentialResolver = botocore_session.get_component("credential_provider")
    assume_role_cache_dir = os.path.join(os.path.expanduser("~"), ".aws", "cli", "cache")
    provider = credential_provider.get_provider("assume-role")
    setattr(provider, "cache", credentials.JSONFileCache(assume_role_cache_dir))
    # SSOProvider's token_cache isn't publicly accessible, so we have to remove the old instance and insert a new one
    credential_provider.remove("sso")
    sso_token_cache_dir = os.path.join(os.path.expanduser("~"), ".aws", "sso", "cache")
    sso_provider_args: Dict["str", Any] = {
        "load_config": lambda: botocore_session.full_config,
        "client_creator": botocore_session.create_client,
        "token_cache": credentials.JSONFileCache(sso_token_cache_dir),
    }
    if profile_name:
        sso_provider_args["profile_name"] = profile_name
    sso_provider = SSOProvider(**sso_provider_args)
    credential_provider.insert_after("assume-role", sso_provider)
    session_args: Dict["str", Any] = {"botocore_session": botocore_session}
    if role_arn:
        client = boto3.Session(**session_args).client("sts")
        response = client.assume_role(RoleArn=role_arn, RoleSessionName="TODO")
        print("Obtained credentials for identity: %s", client.get_caller_identity()["Arn"])
        session_args["aws_access_key_id"] = response["Credentials"]["AccessKeyId"]
        session_args["aws_secret_access_key"] = response["Credentials"]["SecretAccessKey"]
        session_args["aws_session_token"] = response["Credentials"]["SessionToken"]
    sesh = boto3.Session(**session_args)
    sesh.get_credentials()
    return sesh


def retrieve_data_process_ingest_bucket() -> str:
    my_session = get_session()
    s_3 = my_session.client("s3")
    bucket_list = s_3.list_buckets()["Buckets"]
    print(bucket_list)
    filtered_bucket_list = [bucket["Name"] for bucket in bucket_list if DATA_PROCESSING_INGEST_PREFIX in bucket["Name"]]
    if len(filtered_bucket_list) != ACCEPTED_NUMBER_OF_BUCKETS:
        raise Exception("Incorrect amount of data processing ingest buckets")  # pylint: disable=broad-exception-raised
    data_processing_ingest_bucket = filtered_bucket_list[0]
    return str(data_processing_ingest_bucket)
