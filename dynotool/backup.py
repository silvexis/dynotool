#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (c) CloudZero, Inc. All rights reserved.
# Licensed under the MIT License. See LICENSE file in the project root for full license information.
import os
import time
import timeit
import simplejson as json
from botocore.exceptions import ClientError

from dynotool.utils import get_table_info


def perform_backup(dynamodb, arguments):
    table_info = get_table_info(dynamodb, arguments['<TABLE>'])
    read_capacity = table_info['ProvisionedThroughput']['ReadCapacityUnits']

    dest_file_name = os.path.expanduser(arguments['--file'])
    with open(dest_file_name, 'w', newline='\n') as outfile:
        print('Backing up table {} to {}, format is native, read capacity is {}'.format(arguments['<TABLE>'],
                                                                                        dest_file_name,
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

                # print some cute status indicators. Use '.', '*' or '!' depending on how much capacity
                # is being consumed.
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
