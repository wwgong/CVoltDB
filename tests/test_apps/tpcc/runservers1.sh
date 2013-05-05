# Usage: runservers1 tpcb jan16P catalog-cv for example, to use 
#    run_tpcb2.sh with ../jan16P voltdb and use catalog-cv
# or runservers1 tpcc jan16P catalog to run tpcc on 1 host
# of runservers1 tpcc jan16P catalog -ea
echo running run_${1}1.sh $2 server$4 after $3
bash run_${1}1.sh $2 $3 > catalog.log
bash run_${1}1.sh $2 server$4 > server.log
