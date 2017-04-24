#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (c) CloudZero, Inc. All rights reserved.
# Licensed under the MIT License. See LICENSE file in the project root for full license information.
"""
CloudZero Dyn-O-Tool!

Tools for making life with DynamoDB easier

Usage:
    dynotool info <TABLE> [--profile <PROFILE>]
    dynotool head <TABLE> [--profile <PROFILE>]
    dynotool copy <SRC_TABLE> <DEST_TABLE> [--profile <PROFILE>]
    dynotool export <TABLE> --type <TYPE> --out <FILE> [--num <RECORDS> --profile <PROFILE>]
    

Options:
    -? --help               Usage help.
    info                    Get info on the specified table
    head                    Get the first 20 records from the table
    --type <TYPE>           Export type, supported types are TSV and CSV
    --num <RECORDS>         Number of records to export
    --out <FILE>            File to export data to
    --profile <PROFILE>     AWS Profile to use (optional)
"""

from __future__ import print_function, unicode_literals, absolute_import

import os
import csv
from pprint import pprint

import boto3
import time
from botocore.exceptions import ClientError
from pynamodb.attributes import MapAttribute, UnicodeAttribute, NumberAttribute
import sys
from docopt import docopt

from dynotool import __version__


def get_table_info(client, table_name):
    try:
        result = client.describe_table(TableName=table_name)
        table_info = result.get('Table')
        return table_info
    except ClientError:
        return None


def main():
    arguments = docopt(__doc__)
    print('CloudZero Dyn-O-Tool! v{}'.format(__version__))
    print('-' * 120)

    aws_profile = arguments.get('--profile') or 'default'

    session = boto3.Session(profile_name=aws_profile)
    client = session.client('dynamodb')

    if arguments['info']:
        table_info = get_table_info(client, arguments['<TABLE>'])
        if table_info:
            print('Table {} is {}'.format(arguments['<TABLE>'], table_info['TableStatus']))
            print('   Contains roughly {:,} items and {:,.2f} MB'.format(table_info['ItemCount'],
                                                                         table_info['TableSizeBytes'] / (1024 * 1024)))
    elif arguments['head']:
        table_info = get_table_info(client, arguments['<TABLE>'])
        if table_info:
            result = client.scan(TableName=arguments['<TABLE>'], Limit=20)
            if result['Count'] > 0:
                for record in result['Items']:
                    print(record)

    elif arguments['copy']:
        source_table = arguments['<SRC_TABLE>']
        dest_table = arguments['<DEST_TABLE>']
        table_list = client.list_tables()['TableNames']
        if source_table in table_list and dest_table not in table_list:
            source_table_info = get_table_info(client, source_table)

            try:
                del source_table_info['ProvisionedThroughput']['NumberOfDecreasesToday']
                del source_table_info['ProvisionedThroughput']['LastIncreaseDateTime']
                del source_table_info['ProvisionedThroughput']['LastDecreaseDateTime']
            except KeyError:
                pass

            dest_table_config = {'TableName': dest_table,
                                 'AttributeDefinitions': source_table_info['AttributeDefinitions'],
                                 'KeySchema': source_table_info['KeySchema'],
                                 'ProvisionedThroughput': source_table_info['ProvisionedThroughput']}

            if source_table_info.get('LocalSecondaryIndexes'):
                dest_table_config['LocalSecondaryIndexes'] = source_table_info['LocalSecondaryIndexes']
            if source_table_info.get('GlobalSecondaryIndexes'):
                dest_table_config['GlobalSecondaryIndexes'] = source_table_info['GlobalSecondaryIndexes']
            if source_table_info.get('StreamSpecification'):
                dest_table_config['StreamSpecification'] = source_table_info['StreamSpecification']

            print('Extracted source table configuration:')
            pprint(dest_table_config)
            dest_table_info = client.create_table(**dest_table_config)['TableDescription']
            print('Creating {}'.format(dest_table), end='')
            wait_count = 0
            while dest_table_info['TableStatus'] != 'ACTIVE':
                print('.', end='', flush=True)
                time.sleep(0.2)
                dest_table_info = get_table_info(client, dest_table)
                wait_count += 1
                if wait_count > 50:
                    print('ERROR: Table creation taking too long, unsure why, exiting.')
                    pprint(dest_table_info)
                    sys.exit(1)

            print('success')

            results = []
            scan_result = {'Count': -1, 'ScannedCount': 1}
            print('Reading data from {}'.format(source_table), end='')
            while scan_result['Count'] != scan_result['ScannedCount']:
                scan_result = client.scan(TableName=source_table, Select='ALL_ATTRIBUTES')
                results += scan_result['Items']
                print('.', end='', flush=True)

            print(".Done\n{} Records loaded from {}".format(len(results), source_table))

            # todo Convert to use batch write item
            print("Loading records into {}".format(dest_table), end='')
            write_count = 0
            for item in results:
                client.put_item(TableName=dest_table, Item=item)
                print('.', end='', flush=True)
                write_count += 1
            print('Done! {} records written'.format(write_count))
        else:
            print('Destination table {} already exists, unable to complete copy.'.format(dest_table))
    elif arguments['export']:
        table_info = get_table_info(client, arguments['<TABLE>'])
        read_capacity = table_info['ProvisionedThroughput']['ReadCapacityUnits']

        if arguments['--type'].lower() == 'csv':
            delimiter = ','
        elif arguments['--type'].lower() == 'tsv':
            delimiter = '\t'
        else:
            print('Unsupported export type {}'.format(arguments['--type']))
            return 1

        csv_filename = os.path.expanduser(arguments['--out'])
        field_names = ['event_id', 'event_timestamp', 'event_time', 'event_type', 'event_version',
                       'event_name', 'event_region', 'env_id', 'event_size', 'event', 'source_env_id',
                       'source_message', 'source_message_time']
        with open(csv_filename, 'w', newline='') as csvfile:
            csvwriter = csv.DictWriter(csvfile, fieldnames=field_names, delimiter=delimiter)
            csvwriter.writeheader()

            print('Exporting {} to {}'.format(arguments['<TABLE>'], arguments['--type']))

            kwargs = {}
            done = False
            ix = 0
            rows_received = 0
            rows_to_get = int(arguments.get('--num') or 0)
            max_capacity = 0
            while not done:
                ix += 1
                if rows_to_get:
                    kwargs['Limit'] = rows_to_get - rows_received

                result = client.scan(TableName=arguments['<TABLE>'], ReturnConsumedCapacity="TOTAL",
                                     Select="ALL_ATTRIBUTES", **kwargs)
                consumed_capacity = result['ConsumedCapacity']['CapacityUnits']
                max_capacity = max(max_capacity, consumed_capacity)

                if result.get('LastEvaluatedKey'):
                    kwargs['ExclusiveStartKey'] = result.get('LastEvaluatedKey')
                else:
                    done = True

                if result['Count'] > 0:
                    data = {}
                    for record in result['Items']:
                        for k, v in record.items():
                            if k in field_names:
                                if v.get('M'):
                                    data[k] = MapAttribute().deserialize(v['M'])
                                elif v.get('S'):
                                    data[k] = UnicodeAttribute().deserialize(v['S'])
                                elif v.get('N'):
                                    data[k] = NumberAttribute().deserialize(v['N'])

                        if consumed_capacity / read_capacity >= 0.9:
                            status_char = '!'
                        elif consumed_capacity / read_capacity >= 0.65:
                            status_char = '*'
                        else:
                            status_char = '.'

                        if rows_received % 1000 == 0:
                            print(rows_received, end='', flush=True)
                        else:
                            print(status_char, end='', flush=True)
                        rows_received += 1

                        csvwriter.writerow(data)

                if rows_to_get and (rows_received >= rows_to_get):
                    done = True

        print('\nExport complete: {} rows exported to {} '
              '(in {} request(s), max consumed capacity: {})'.format(rows_received,
                                                                     csv_filename,
                                                                     ix,
                                                                     max_capacity))

    return 0


if __name__ == "__main__":
    status = main()
    sys.exit(status)
