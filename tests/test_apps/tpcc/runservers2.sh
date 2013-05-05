# Usage: runservers2 jan16P for example, to use 
#    run2h.sh with ../jan16P voltdb 
echo about to do, on cv3: bash run2h.sh $1 catalog, then server
ssh cv3.local << EOF 
cd tpcc-run
bash run2h.sh $1 catalog > catalog.log
bash run2h.sh $1 server-ea > server.log&
EOF
echo cv3 started
bash run2h.sh $1 catalog > catalog.log
bash run2h.sh $1 server-ea > server.log
