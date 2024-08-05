spark-submit \
--class student45_hw05 \
--master yarn \
--deploy-mode cluster \
--executor-memory 1G \
--num-executors 3 \
student45-hw05-assembly-1.0.jar