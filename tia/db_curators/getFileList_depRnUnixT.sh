#!/bin/bash
echo "depth/extracted/subdirs!!"
sleep 5
echo "renaming files to unixtimestamp"


dirs=$(ls -d $PWD/_misc/db/mtgox_depth/depth/extracted/*)
for dir in $dirs; do
	pushd $dir 	#go into that dir
	pwd=$(pwd)
	echo "now in: " $pwd
	files=$(ls|awk -F. '{print$1}') 	#filenames without extensions
	for fileName in $files; do
		year=$(echo $fileName|awk -F- '{print$1}')
		month=$(echo $fileName|awk -F- '{print$2}')
		day=$(echo $fileName|awk -F- '{print$3}')
		hour_minute=$(echo $fileName|awk -F- '{print$4}')
		
		timestamp=$(date -d"$year-$month-$day $hour_minute" +%s)
		mv $fileName.json $timestamp.json
	done
	
	popd
done
