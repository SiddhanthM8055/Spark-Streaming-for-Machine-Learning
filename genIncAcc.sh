#!/bin/sh

BATCHSIZE=100

while [ $BATCHSIZE -lt 600 ]
do
   echo $BATCHSIZE
   python3 myStream.py -f spam -b $BATCHSIZE -sr 1  &
   /opt/spark/bin/spark-submit main.py -sr 1 -m "MNB${BATCHSIZE}" &
   wait
   BATCHSIZE=`expr $BATCHSIZE + 100`
done

while [ $BATCHSIZE -lt 1100 ]
do
   echo $BATCHSIZE
   python3 myStream.py -f spam -b $BATCHSIZE -sr 5  &
   /opt/spark/bin/spark-submit main.py -sr 5 -m "MNB${BATCHSIZE}" &
   wait
   BATCHSIZE=`expr $BATCHSIZE + 100`
done

while [ $BATCHSIZE -lt 1600 ]
do
   echo $BATCHSIZE
   python3 myStream.py -f spam -b $BATCHSIZE -sr 10  &
   /opt/spark/bin/spark-submit main.py -sr 8 -m "MNB${BATCHSIZE}" &
   wait
   BATCHSIZE=`expr $BATCHSIZE + 100`
done