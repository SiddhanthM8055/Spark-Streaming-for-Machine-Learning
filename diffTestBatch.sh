#!/bin/sh
echo "$PATH"
export PATH=$PATH:/home/pes1ug19cs481/BD/MLSS/
echo "$PATH"

BATCHSIZE=100

while [ $BATCHSIZE -lt 1600 ]
do
   echo $BATCHSIZE
   BATCHSIZE=`expr $BATCHSIZE + 100`
   python3 myStream.py -f spam -b $BATCHSIZE -sr 3 -t True &
   /opt/spark/bin/spark-submit main.py -sr 3 -m 'MLP' -t True &
   wait
done