#!/bin/bash
echo "running filtering than stage3"
echo "starting filtering"
python db_filter.py
echo "starting stage3"
python db_curator.py stage3
