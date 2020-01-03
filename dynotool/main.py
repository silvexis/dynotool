#!/usr/bin/env python
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
    dynotool export <TABLE> [--format <format> --file <file> --profile <name>]
    dynotool import <TABLE> --file <file> [--profile <name>]
    dynotool wipe <TABLE> [--profile <name>]
    dynotool truncate <TABLE> [--filter <filter>] [--profile <name>]


Options:
    -? --help               Usage help.
    list                    List all DyanmoDB tables currently provisioned.
    info                    Get info on the specified table.
    head                    Get the first 20 records from the table.
    copy                    Copy the data from SRC TABLE to DEST TABLE
    export                  Export TABLE to JSON
    import                  Import file into TABLE
    wipe                    Wipe an existing table by recreating it (delete and create)
    truncate                Wipe an existing table by deleting all records
    --format <format>       JSON or CSV [default: json]
    --file <file>           File to import or export data to, defaults to table name.
    --profile <profile>     AWS Profile to use (optional) [default: default].
    --filter <filter>       A filter to apply to the operation. The syntax depends on the operation.
"""

from __future__ import print_function, unicode_literals, absolute_import

import csv
import os
import sys
import time
import timeit
from pprint import pprint
from random import randrange

import boto3
import simplejson as json
from botocore.exceptions import ClientError
from docopt import docopt

from dynotool import __version__
from dynotool.utils import deserialize_dynamo_data, get_table_info, serialize_to_json

EXPORT_TYPE_SEQUENTIAL = "sequential"
EXPORT_TYPE_PARALLEL = "parallel"
EXPORT_TYPES = (EXPORT_TYPE_SEQUENTIAL, EXPORT_TYPE_PARALLEL)


def check_input_output_target(output_destination, file_format):
    if not output_destination:
        return None, None

    if output_destination.lower().startswith('s3://'):
        return output_destination[5:], "S3"
    else:
        if not output_destination.endswith(f'.{file_format}'):
            output_destination += f'.{file_format}'

        return os.path.expanduser(output_destination), "file"


def export_write_header(outfile, export_format):
    if export_format == "json":
        outfile.write('[\n')
    elif export_format == "csv":
        outfile.writeheader()


def export_write_footer(outfile, export_format):
    if export_format == "json":
        outfile.write('\n]')
    elif export_format == "csv":
        pass


def export_write_row(record, row_number, writer, export_format):
    record = deserialize_dynamo_data(record)
    if export_format == "json":
        try:
            json_record = json.dumps(record, default=serialize_to_json)
        except TypeError as error:
            print(fr"ERROR: Data can not be serialized to JSON ¯\_(ツ)_/¯ ({error})")
            pprint(record)
            sys.exit(1)

        if row_number > 0:
            writer.write(",\n  {}".format(json_record))
        else:
            writer.write("  {}".format(json_record))
    elif export_format == "csv":
        try:
            writer.writerow(record)
        except ValueError:
            # Typically this happens when we come across a record that doesn't fit the schema
            # ValueError: dict contains fields not in fieldnames: ....
            print("-", end='', flush=True)


def main():
    arguments = docopt(__doc__)
    print('CloudZero Dyn-O-Tool! v{}'.format(__version__))
    print('-' * 120)

    aws_profile = arguments['--profile']

    session = boto3.Session(profile_name=aws_profile)
    ddb_client = session.client('dynamodb')
    ddb_resource = session.resource('dynamodb')

    if arguments['list']:
        done = False
        while not done:
            response = ddb_client.list_tables()
            table_list = response['TableNames']
            for table_name in table_list:
                table_info = get_table_info(ddb_client, table_name)
                print("{:<70} {} ~{:>10} records ({:,.2f} mb)".format(table_name, table_info['TableStatus'],
                                                                      table_info['ItemCount'],
                                                                      table_info['TableSizeBytes'] / (1024 * 1024)))
            if len(table_list) <= 100:
                done = True

    elif arguments['info']:
        table_info = get_table_info(ddb_client, arguments['<TABLE>'])
        if table_info:
            print('Table {} is {}'.format(arguments['<TABLE>'], table_info['TableStatus']))
            print('   Contains roughly {:,} items and {:,.2f} MB'.format(table_info['ItemCount'],
                                                                         table_info['TableSizeBytes'] / (1024 * 1024)))
    elif arguments['head']:
        table_info = get_table_info(ddb_client, arguments['<TABLE>'])
        if table_info:
            result = ddb_client.scan(TableName=arguments['<TABLE>'], Limit=20)
            if result['Count'] > 0:
                for record in result['Items']:
                    print(record)

    elif arguments['copy']:
        source_table = arguments['<SRC_TABLE>']
        dest_table = arguments['<DEST_TABLE>']
        table_list = ddb_client.list_tables()['TableNames']
        if source_table in table_list and dest_table not in table_list:
            source_table_info = get_table_info(ddb_client, source_table)

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
            dest_table_info = ddb_client.create_table(**dest_table_config)['TableDescription']
            print('Creating {}'.format(dest_table), end='')
            wait_count = 0
            while dest_table_info['TableStatus'] != 'ACTIVE':
                print('.', end='', flush=True)
                time.sleep(0.2)
                dest_table_info = get_table_info(ddb_client, dest_table)
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
                scan_result = ddb_client.scan(TableName=source_table, Select='ALL_ATTRIBUTES')
                results += scan_result['Items']
                print('.', end='', flush=True)

            print(".Done\n{} Records loaded from {}".format(len(results), source_table))

            # todo Convert to use batch write item
            print("Loading records into {}".format(dest_table), end='')
            write_count = 0
            for item in results:
                ddb_client.put_item(TableName=dest_table, Item=item)
                print('.', end='', flush=True)
                write_count += 1
            print('Done! {} records written'.format(write_count))
        else:
            print('Destination table {} already exists, unable to complete copy.'.format(dest_table))
    elif arguments['export']:
        export_dest, export_type = check_input_output_target(arguments['--file'] or arguments['<TABLE>'],
                                                             arguments['--format'])
        table_info = get_table_info(ddb_client, arguments['<TABLE>'])
        if table_info is None:
            print(f"Could not find or load {arguments['<TABLE>']}, check your AWS permissions")
            sys.exit()
        provisioned_throughput = table_info.get('ProvisionedThroughput') or {}
        read_capacity = provisioned_throughput.get('ReadCapacityUnits')

        file_format = arguments.get('--format')

        if export_type == 'file':
            with open(export_dest, 'w', newline='\n') as outfile:
                print('Exporting {} to {}, read capacity is {}'.format(arguments['<TABLE>'],
                                                                       file_format,
                                                                       read_capacity or "infinite"))
                kwargs = {}
                done = False
                request_count = 0
                rows_imported = 0
                max_capacity = 0
                retries = 0
                start = timeit.default_timer()

                if file_format == "json":
                    writer = outfile
                elif file_format == "csv":
                    result = ddb_client.scan(TableName=arguments['<TABLE>'], Limit=1)
                    fieldnames = result['Items'][0].keys()
                    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                else:
                    print(f"ERROR: Unknown export format {file_format}")
                    sys.exit(1)

                export_write_header(writer, export_format=file_format)

                while not done:
                    try:
                        request_count += 1

                        result = ddb_client.scan(TableName=arguments['<TABLE>'],
                                                 ReturnConsumedCapacity="TOTAL",
                                                 Select="ALL_ATTRIBUTES", **kwargs)

                        # FilterExpression="metric_type = :metric_type",
                        # ExpressionAttributeValues={":metric_type": {"S": "AWS/RDS"}}

                        consumed_capacity = result['ConsumedCapacity']['CapacityUnits']
                        max_capacity = max(max_capacity, consumed_capacity)

                        if result.get('LastEvaluatedKey'):
                            kwargs['ExclusiveStartKey'] = result.get('LastEvaluatedKey')
                        else:
                            done = True

                        for record in result['Items']:
                            export_write_row(record, rows_imported, writer, export_format=file_format)
                            rows_imported += 1

                        # print some cute status indicators. Use '.', '*' or '!' depending on how much capacity
                        # is being consumed.
                        if read_capacity == 0:  # Infinite capacity
                            print("_", end='', flush=True)
                        elif consumed_capacity / read_capacity >= 0.9:
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

                export_write_footer(outfile, export_format=file_format)

            stop = timeit.default_timer()
            total_time = stop - start
            avg_row_processing_time = rows_imported / total_time
            print(f'\nExport complete, output file: {export_dest}\n'
                  f'{rows_imported} rows exported in {total_time:.2f} seconds (~{avg_row_processing_time:.2f} rps) '
                  f'in {request_count} request(s), max consumed capacity: {max_capacity}')

    elif arguments['import']:
        input_source, import_type = check_input_output_target(arguments['--file'], arguments['--format'])
        write_capacity = 0
        target_table_name = arguments['<TABLE>']

        file_format = arguments['--format'] or os.path.splitext(arguments['--file'])
        if file_format.lower() != "json":
            print("Only JSON import file types are supported.")
            sys.exit()

        print('Importing {} to table {} ({} write capacity)'.format(input_source,
                                                                    target_table_name,
                                                                    write_capacity or "infinite"))
        start = timeit.default_timer()
        rows_imported = 0

        if import_type == 'file':

            table = ddb_resource.Table(target_table_name)
            import_data = json.load(open(input_source, 'r'))
            with table.batch_writer() as batch:
                for item in import_data:
                    batch.put_item(Item=item)
                    print(".", end='', flush=True)
                    rows_imported += 1

        elif import_type == 'S3':
            print(f"S3 import not yet supported")

        stop = timeit.default_timer()
        total_time = stop - start
        print('\nImport complete: {} rows imported in {:.2f} seconds'.format(rows_imported,
                                                                             total_time))
    elif arguments['wipe']:
        table_name = arguments['<TABLE>']
        print('Wiping table {} (via remove and recreate)'.format(table_name))
        response = ddb_client.describe_table(TableName=table_name)
        table_definition = extract_table_definition(response['Table'])
        print(' - Removing table')
        response = ddb_client.delete_table(TableName=table_name)
        ddb_client.get_waiter('table_not_exists').wait(TableName=table_name)
        print(' - Table Removed, Recreating')
        response = ddb_client.create_table(**table_definition)
        ddb_client.get_waiter('table_exists').wait(TableName=table_name)
        print(' - Table recreated, finished')
    elif arguments['truncate']:
        table_name = arguments['<TABLE>']
        print('Wiping table {} (by truncating)'.format(table_name))
        result = delete_all_items(session, table_name, arguments['--filter'])
        print(result)

    return 0


def delete_all_items(session, table_name, filter=None):
    client = session.client('dynamodb')
    resource = session.resource('dynamodb')
    table = resource.Table(table_name)
    # Deletes all items from a DynamoDB table.
    # You need to confirm your intention by pressing Enter.
    response = client.describe_table(TableName=table_name)
    aprox_item_count = response['Table']['ItemCount']
    keys = [k['AttributeName'] for k in response['Table']['KeySchema']]
    scan_args = {}

    if filter:
        scan_args['ScanFilter'] = json.loads(filter)
    response = table.scan(**scan_args)

    items = response['Items']
    number_of_items = len(items)
    if number_of_items == 0:  # no items to delete
        print("Table '{}' is empty.".format(table_name))
        return

    print("You are about to delete {} items (out of {}) from table '{}'.".format(number_of_items,
                                                                                 aprox_item_count,
                                                                                 table_name))
    print("Sample Event:")
    pprint(items[randrange(0, number_of_items)])
    input("Press Enter to continue...")

    count = 0
    item = None
    while response.get('LastEvaluatedKey') or count == 0:
        count += 1
        try:
            with table.batch_writer() as batch:
                for item in items:
                    key_dict = {k: item[k] for k in keys}
                    # print("Deleting {}".format(key_dict))
                    print('.', end='', flush=True)
                    batch.delete_item(Key=key_dict)
                    count += 1
        except Exception as error:
            print(error)
            print(item)
        if response.get('LastEvaluatedKey'):
            scan_args['ExclusiveStartKey'] = response.get('LastEvaluatedKey')
        response = table.scan(**scan_args)
        items = response['Items']
        number_of_items = len(items)
        print("-" * 120)
        print('Found {} more to delete'.format(number_of_items))

    return count


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
