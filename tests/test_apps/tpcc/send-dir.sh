# Usage: send-dir jan16P, from cv2, home dir
scp -r $1 eoneil@cv1.local:$1
scp -r $1 eoneil@cv3.local:$1
scp -r $1 eoneil@cv4.local:$1
scp -r $1 eoneil@cv0.local:$1


