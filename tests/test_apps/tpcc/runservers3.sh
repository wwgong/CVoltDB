# Usage: runservers3 tpcb jan16P for example, to use run_tpcb3.sh with ../jan16P voltdb
ssh cv1.local << EOF 
cd tpc-cv
bash run_${1}3.sh $2 catalog-cv > catalog.log
bash run_${1}3.sh $2 server > server.log&
EOF
echo cv1 started
ssh cv3.local << EOF 
cd tpc-cv
bash run_${1}3.sh $2 catalog-cv > catalog.log
bash run_${1}3.sh $2 server > server.log&
EOF
echo cv3 started
bash run_${1}3.sh $2 catalog-cv > catalog.log
bash run_${1}3.sh $2 server > server.log
