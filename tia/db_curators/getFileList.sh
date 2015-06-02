#echo "starting renamint to unixtime"
#bash getFileList_depRnUnixT.sh
#echo "finished renaming"

#new depth
echo "writing depth to allFilesList.txt"
dirsNew=$(ls -d $PWD/_misc/db/mtgox_depth/depth/extracted/*)

for i in $dirsNew; do
	echo "now in: " $i
	./listdir $i >> _misc/db/allFilesList.txt
done