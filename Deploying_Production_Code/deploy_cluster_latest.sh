aws emr create-cluster --release-label emr-5.33.1  --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.xlarge InstanceGroupType=CORE,InstanceCount=1,InstanceType=m5.xlarge --use-default-roles --applications Name=Spark  --name “testcli” --log-uri s3://aws-logs-461885750495-us-west-1/elasticmapreduce/ 

aws emr describe-cluster --cluster-id j-107L34TIBRK2

aws emr add-steps --cluster-id j-9ZDMCFI38DE2 --steps Type=CUSTOM_JAR,Name="Spark Program",Jar="command-runner.jar",ActionOnFailure=CONTINUE,Args=["spark-submit",s3://rachaelspringboard20211009bucket/scaled_prototype.py,s3://rachaelspringboard20211009bucket/,s3://rachaelspringboard20211009bucket/output2/]

aws emr put-auto-termination-policy --cluster-id j-9ZDMCFI38DE2 --auto-termination-policy IdleTimeout=60
