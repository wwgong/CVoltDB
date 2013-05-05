# 4 hosts, 2 clients (cv1 and this host)
# Usage: runclients4h-2c tpcb jan16P 15 for example, to use run_tpcb4.sh with ../jan16P voltdb
# and 15% MP
ssh cv1.local << EOF 
cd tpc-cv
bash run_${1}4.sh $2 client $3 > client${3}-4h-2c-b.log&
EOF
echo cv1 started
bash run_${1}4.sh $2 client $3 > client${3}-4h-2c-a.log&
