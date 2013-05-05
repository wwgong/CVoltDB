# Usage: runservers4 jan16P  for example, 
#  to use run4h.sh with ../jan16P voltdb 
# this assumes the server and client dirs are
# current on all 4 systems--use send-dir
# and/or update-dirs to make this true
echo about to run, on cv1: bash run4h.sh $1 catalog
ssh cv1.local << EOF 
cd tpcc-run
bash run4h.sh $1 catalog > catalog.log
bash run4h.sh $1 server > server.log&
EOF
echo cv1 started
ssh cv3.local << EOF 
cd tpcc-run
bash run4h.sh $1 catalog > catalog.log
bash run4h.sh $1 server > server.log&
EOF
echo cv3 started
ssh cv4.local << EOF 
cd tpcc-run
bash run4h.sh $1 catalog > catalog.log
bash run4h.sh $1 server > server.log&
EOF
echo cv4 started
bash run4h.sh $1 catalog > catalog.log
bash run4h.sh $1 server > server.log
