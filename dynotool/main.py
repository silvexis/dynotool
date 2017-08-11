#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (c) CloudZero, Inc. All rights reserved.
# Licensed under the MIT License. See LICENSE file in the project root for full license information.
"""
CloudZero Dyn-O-Tool!

Tools for making life with DynamoDB easier

Usage:
    dynotool list [--profile <name>]
    dynotool info <TABLE> [--profile <name>]
    dynotool head <TABLE> [--profile <name>]
    dynotool copy <SRC_TABLE> <DEST_TABLE> [--profile <name>]
    dynotool export <TABLE> [--file <file> --type <type> --profile <name> --namespace <name> --segments <num>]
    dynotool import <TABLE> --file <file> [--profile <name>]
    dynotool wipe <TABLE> [--profile <name>]


Options:
    -? --help               Usage help.
    list                    List all DyanmoDB tables currently provisioned.
    info                    Get info on the specified table.
    head                    Get the first 20 records from the table.
    copy                    Copy the data from SRC TABLE to DEST TABLE
    export                  Export TABLE to file or S3 bucket
    import                  Import file or S3 bucket into TABLE
    wipe                    Wipe an existing DynamoDB Table by recreating it (delete and create)
    --type <type>           Export type, either sequential or parallel [default: sequential].
    --file <file>           File or S3 bucket to import or export data to, defaults to table name.
    --profile <profile>     AWS Profile to use (optional) [default: default].
    --namespace <name>      Namespace to use when calling remote functions [default: dev].
    --segments <num>        Number of segments to break a parallel export into [default: 10].
"""

from __future__ import print_function, unicode_literals, absolute_import

import json
import timeit
from pprint import pprint

import boto3
import time
from botocore.exceptions import ClientError
import sys
from docopt import docopt

from dynotool import __version__

EXPORT_TYPE_SEQUENTIAL = "sequential"
EXPORT_TYPE_PARALLEL = "parallel"
EXPORT_TYPES = (EXPORT_TYPE_SEQUENTIAL, EXPORT_TYPE_PARALLEL)


def get_table_info(client, table_name):
    try:
        result = client.describe_table(TableName=table_name)
        table_info = result.get('Table')
        return table_info
    except ClientError:
        return None


def check_input_output_target(output_destination):
    if not output_destination:
        return None, None

    if output_destination.lower().startswith('s3://'):
        return output_destination[5:], "S3"
    else:
        if not output_destination.endswith('.json'):
            output_destination += '.json'

        return output_destination, "file"


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def main():
    arguments = docopt(__doc__)
    print('CloudZero Dyn-O-Tool! v{}'.format(__version__))
    print('-' * 120)

    aws_profile = arguments['--profile']
    namespace = arguments['--namespace']

    session = boto3.Session(profile_name=aws_profile)
    dynamodb = session.client('dynamodb')
    lam = session.client('lambda')
    s3 = session.resource('s3')

    if arguments['list']:
        done = False
        while not done:
            response = dynamodb.list_tables()
            table_list = response['TableNames']
            for table_name in table_list:
                table_info = get_table_info(dynamodb, table_name)
                print("{:<40} {} ~{:>10} records ({:,.2f} mb)".format(table_name, table_info['TableStatus'],
                                                                      table_info['ItemCount'],
                                                                      table_info['TableSizeBytes'] / (1024 * 1024)))
            if len(table_list) <= 100:
                done = True

    elif arguments['info']:
        table_info = get_table_info(dynamodb, arguments['<TABLE>'])
        if table_info:
            print('Table {} is {}'.format(arguments['<TABLE>'], table_info['TableStatus']))
            print('   Contains roughly {:,} items and {:,.2f} MB'.format(table_info['ItemCount'],
                                                                         table_info['TableSizeBytes'] / (1024 * 1024)))
    elif arguments['head']:
        table_info = get_table_info(dynamodb, arguments['<TABLE>'])
        if table_info:
            result = dynamodb.scan(TableName=arguments['<TABLE>'], Limit=20)
            if result['Count'] > 0:
                for record in result['Items']:
                    print(record)

    elif arguments['copy']:
        source_table = arguments['<SRC_TABLE>']
        dest_table = arguments['<DEST_TABLE>']
        table_list = dynamodb.list_tables()['TableNames']
        if source_table in table_list and dest_table not in table_list:
            source_table_info = get_table_info(dynamodb, source_table)

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
            dest_table_info = dynamodb.create_table(**dest_table_config)['TableDescription']
            print('Creating {}'.format(dest_table), end='')
            wait_count = 0
            while dest_table_info['TableStatus'] != 'ACTIVE':
                print('.', end='', flush=True)
                time.sleep(0.2)
                dest_table_info = get_table_info(dynamodb, dest_table)
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
                scan_result = dynamodb.scan(TableName=source_table, Select='ALL_ATTRIBUTES')
                results += scan_result['Items']
                print('.', end='', flush=True)

            print(".Done\n{} Records loaded from {}".format(len(results), source_table))

            # todo Convert to use batch write item
            print("Loading records into {}".format(dest_table), end='')
            write_count = 0
            for item in results:
                dynamodb.put_item(TableName=dest_table, Item=item)
                print('.', end='', flush=True)
                write_count += 1
            print('Done! {} records written'.format(write_count))
        else:
            print('Destination table {} already exists, unable to complete copy.'.format(dest_table))
    elif arguments['export']:
        if arguments['--type'] not in EXPORT_TYPES:
            print('Unsupported export type {}'.format(arguments['--type']))
            sys.exit(1)

        input_source, input_type = check_input_output_target(arguments['--file'] or arguments['<TABLE>'])

        if arguments['--type'] == EXPORT_TYPE_PARALLEL:
            payload = {
                "s3_bucket": input_source,
                "src_table": arguments['<TABLE>'],
                "total_segments": arguments['--segments']
            }
            response = lam.invoke(FunctionName='dyn-o-tool-{}-dump-table-launcher'.format(namespace),
                                  InvocationType='Event',
                                  Payload=json.dumps(payload))
            if response['StatusCode'] == 202:
                print('Parallel table dump started for {} to s3://{}'.format(arguments['<TABLE>'],
                                                                             input_source))
            else:
                print("Something went wrong, I'm not sure what sorry!")
                pprint(response)

        elif arguments['--type'] == EXPORT_TYPE_SEQUENTIAL:
            table_info = get_table_info(dynamodb, arguments['<TABLE>'])
            read_capacity = table_info['ProvisionedThroughput']['ReadCapacityUnits']

            if input_type == 'file':
                with open(input_source, 'w', newline='\n') as outfile:
                    print('Exporting {} to {}, read capacity is {}'.format(arguments['<TABLE>'],
                                                                           arguments['--type'],
                                                                           read_capacity))
                    kwargs = {}
                    done = False
                    request_count = 0
                    rows_received = 0
                    max_capacity = 0
                    retries = 0
                    start = timeit.default_timer()
                    while not done:
                        try:
                            request_count += 1

                            result = dynamodb.scan(TableName=arguments['<TABLE>'],
                                                   ReturnConsumedCapacity="TOTAL",
                                                   Select="ALL_ATTRIBUTES", **kwargs)
                            consumed_capacity = result['ConsumedCapacity']['CapacityUnits']
                            max_capacity = max(max_capacity, consumed_capacity)

                            if result.get('LastEvaluatedKey'):
                                kwargs['ExclusiveStartKey'] = result.get('LastEvaluatedKey')
                            else:
                                done = True

                            for record in result['Items']:
                                outfile.write("{}\n".format(json.dumps(record)))
                                rows_received += 1

                            if consumed_capacity / read_capacity >= 0.9:
                                print("!", end='', flush=True)
                            elif consumed_capacity / read_capacity >= 0.65:
                                print("*", end='', flush=True)
                            else:
                                print(".", end='', flush=True)

                        except ClientError as err:
                            if err.response['Error']['Code'] not in ('ProvisionedThroughputExceededException',
                                                                     'ThrottlingException'):
                                raise
                            print('<' * retries)
                            time.sleep(2 ** retries)
                            retries += 1

                stop = timeit.default_timer()
                total_time = stop - start
                avg_row_processing_time = rows_received / total_time
                print('\nExport complete: {} rows exported in {:.2f} seconds (~{:.2f} rps) '
                      'in {} request(s), max consumed capacity: {}'.format(rows_received,
                                                                           total_time,
                                                                           avg_row_processing_time,
                                                                           request_count,
                                                                           max_capacity))
            else:
                print('Other output types for sequential exporter are not supported at this time')

    elif arguments['import']:
        input_source, input_type = check_input_output_target(arguments['--file'])
        # table_info = get_table_info(dynamodb, arguments['<TABLE>'])
        # write_capacity = table_info['ProvisionedThroughput']['WriteCapacityUnits']
        write_capacity = 0
        target_table_name = arguments['<TABLE>']
        print('Importing {} to table {} ({} write capacity)'.format(arguments['--file'],
                                                                    target_table_name,
                                                                    write_capacity))
        bucket = s3.Bucket(input_source)
        done = False
        request_count = 0
        rows_received = 0
        max_capacity = 0
        retries = 0
        start = timeit.default_timer()
        while not done:
            try:
                request_count += 1
                for obj in bucket.objects.all():
                    print(obj.key)
                    response = obj.get()
                    data = response['Body'].read().decode('utf-8')
                    prepared_data = [{'PutRequest': {'Item': json.loads(x)}} for x in data.splitlines()]
                    for chunk in chunks(prepared_data, 25):
                        dynamodb.batch_write_item(RequestItems={target_table_name: chunk})

                done = True

                # consumed_capacity = result['ConsumedCapacity']['CapacityUnits']
                # max_capacity = max(max_capacity, consumed_capacity)
                #
                # if consumed_capacity / write_capacity >= 0.9:
                #     print("!", end='', flush=True)
                # elif consumed_capacity / write_capacity >= 0.65:
                #     print("*", end='', flush=True)
                # else:
                #     print(".", end='', flush=True)

            except ClientError as err:
                if err.response['Error']['Code'] not in ('ProvisionedThroughputExceededException',
                                                         'ThrottlingException'):
                    raise
                print('<' * retries)
                time.sleep(2 ** retries)
                retries += 1

        stop = timeit.default_timer()
        total_time = stop - start
        avg_row_processing_time = rows_received / total_time
        print('\nImport complete: {} rows imported in {:.2f} seconds (~{:.2f} rps) '
              'in {} request(s), max consumed capacity: {}'.format(rows_received,
                                                                   total_time,
                                                                   avg_row_processing_time,
                                                                   request_count,
                                                                   max_capacity))
    elif arguments['wipe']:
        table_name = arguments['<TABLE>']
        print('Wiping table {} (via remove and recreate)'.format(table_name))
        response = dynamodb.describe_table(TableName=table_name)
        table_definition = extract_table_definition(response['Table'])
        print(' - Removing table')
        response = dynamodb.delete_table(TableName=table_name)
        dynamodb.get_waiter('table_not_exists').wait(TableName=table_name)
        print(' - Table Removed, Recreating')
        response = dynamodb.create_table(**table_definition)
        dynamodb.get_waiter('table_exists').wait(TableName=table_name)
        print(' - Table recreated, finished')

    return 0


def extract_table_definition(description):
    read_capacity = description['ProvisionedThroughput']['ReadCapacityUnits']
    write_capacity = description['ProvisionedThroughput']['WriteCapacityUnits']
    table_definition = {'TableName': description['TableName'],
                        'AttributeDefinitions': description['AttributeDefinitions'],
                        'KeySchema': description['KeySchema'],
                        'ProvisionedThroughput': {'ReadCapacityUnits': read_capacity,
                                                  'WriteCapacityUnits': write_capacity}}
    if description.get('LocalSecondaryIndexes'):
        table_definition['LocalSecondaryIndexes'] = description.get('LocalSecondaryIndexes')

    if description.get('GlobalSecondaryIndexes'):
        table_definition['GlobalSecondaryIndexes'] = description.get('GlobalSecondaryIndexes')

    if description.get('StreamSpecification'):
        table_definition['StreamSpecification'] = description.get('StreamSpecification')

    return table_definition

if __name__ == "__main__":
    status = main()
    sys.exit(status)
