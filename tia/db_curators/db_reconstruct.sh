#!/bin/bash
bash getFileList.sh

python db_curator.py writeFirst
python db_curator.py writeSecond
python db_curator.py indexAsks
python db_curator.py indexBids
python db_curator.py writeDeltas

echo "get trades, index them,write to events, index events, create sortedEvents"

