#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (c) CloudZero, Inc. All rights reserved.
# Licensed under the MIT License. See LICENSE file in the project root for full license information.
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError


def serialize_to_json(obj):
    """
    Transform objects to types that can be serialized to JSON
    Args:
        obj:

    Returns:

    """
    if isinstance(obj, set):
        return list(obj)
    raise TypeError


def deserialize_dynamo_data(input_data):
    """
    Given a dict containing the "serialized" data format used by the low-level
    dynamodb APIs, convert it to a standard python dictionary

    Args:
        input_data: (dict) - "serialized" form of a dynamodb record

    Returns:
        (dict) - the "deserialized" form of the input data
    """
    deserializer = TypeDeserializer()
    output_data = {}
    for k, v in input_data.items():
        output_data[k] = deserializer.deserialize(v)
    return output_data


def get_table_info(client, table_name):
    try:
        result = client.describe_table(TableName=table_name)
        table_info = result.get('Table')
        return table_info
    except ClientError:
        return None


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]
