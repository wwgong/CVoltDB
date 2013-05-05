# Usage: runservers4 tpcb jan16P for example, to use run_tpcb4.sh with ../jan16P voltdb
ssh cv1.local << EOF 
ps ax|grep volt|grep -v grep|fold|head -2
EOF
echo cv1 done
ssh cv3.local << EOF 
ps ax|grep volt|grep -v grep|fold|head -2
EOF
echo cv3 done
ssh cv4.local << EOF 
ps ax|grep volt|grep -v grep|fold|head -2
EOF
echo cv4 done
ps ax|grep volt|grep -v grep|fold|head -2
echo cv2 done
