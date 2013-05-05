# copy this dir (tpcc-e) to 5 places as tpcc-run
# we keep tpcc-e for sources, execute in tpcc-run
scp -r * eoneil@cv1.local:tpcc-run
scp -r * eoneil@cv3.local:tpcc-run
scp -r * eoneil@cv4.local:tpcc-run
cp -r * ../tpcc-run
scp -r * eoneil@cv0.local:tpcc-run
