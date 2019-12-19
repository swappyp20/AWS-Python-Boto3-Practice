#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 19 17:34:14 2019

@author: swapnil
"""

import logging
import boto3
from botocore.exceptions import ClientError

#Checking s3 bucket exists using boto3 python SDK
def bucket_exists(s3_bucket_name):
    
    #Intialise boto3 client
    s3 = boto3.client('s3')
    try:
        response = s3.head_bucket(Bucket=s3_bucket_name)
    except ClientError as e:
        logging.debug(e)
        return False
    return response

#Listing s3 bucket objects using boto3 python SDK
def list_bucket_objects(bucket_name):
    # Retrieve the list of bucket objects
    s_3 = boto3.client('s3')
    try:
        response = s_3.list_objects_v2(Bucket=bucket_name)
    except ClientError as e:
        # AllAccessDisabled error == bucket not found
        logging.error(e)
        return None

    # Only return the contents if we found some keys
    if response['KeyCount'] > 0:
        return response['Contents']

    return None

#Crerating s3 bucket using boto3 python SDK
def create_bucket(s3_bucket_name):
     #Intialise boto3 client
    region = 'us-west-2'
    s3 = boto3.client('s3', region_name=region)    
    location = {'LocationConstraint': region}
    response = s3.create_bucket(Bucket=s3_bucket_name, CreateBucketConfiguration=location)
    return response

#Copying s3 bucket objects from another bucket using boto3 python SDK
def copy_object_from_bucket(s3_bucket_name, s3_copy_from_bucket):
    #Intialise boto3 client
    s3 = boto3.resource('s3')
    objects = list_bucket_objects(s3_copy_from_bucket)
    for obj in objects:
        key = obj["Key"]
        copy_source = {
            'Bucket': s3_copy_from_bucket,
            'Key': key
        }
        cpoyd = s3.meta.client.copy(copy_source, s3_bucket_name, key)    
        if cpoyd:
            logging.info(f'Objects in {s3_bucket_name} has copied.')

#Execute Main Code first & calling callable functions
def main():
    
    #bucket name
    s3_bucket_name = 'nomad-dev-website-swapnil'
    s3_copy_from_bucket = 'nomad-dev'
    
    if bucket_exists(s3_bucket_name):
        logging.info(f'{s3_bucket_name} exists & you have permission to access it.')
        copy_object_from_bucket(s3_bucket_name, s3_copy_from_bucket)
    else:
        logging.info(f'{s3_bucket_name} didnt exist or you dont have permission to access it.')
        logging.info(f'{s3_bucket_name} bucket creation happening...kindly wait')
        create_bucket(s3_bucket_name)
        
if __name__ == '__main__':
    main()
        