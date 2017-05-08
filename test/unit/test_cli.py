#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (c) CloudZero, Inc. All rights reserved.
# Licensed under the MIT License. See LICENSE file in the project root for full license information.

from docopt import docopt
import dynotool.main as dynotool


def test_main_cli():
    args = docopt(dynotool.__doc__, ["info", "foobar"])
    assert args["<TABLE>"] == "foobar"
