ssh cv1.local << EOF 
cd tpc-cv
bash run_tpcb2.sh jan16P server > server.log&
EOF
echo cv1 started
bash run_tpcb2.sh jan16P server > server.log&
echo cv2 started
