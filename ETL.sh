prodbigdataserviceac@hddle4:/data/WD/scripts/CRM_Scripts> cat ETL.sh
spark-shell --master local --conf spark.eventLog.enabled=false --conf spark.driver.allowMultipleContexts=true --files "/data/WD/scripts/jaas.conf,/data/WD/scripts/CRM_Scripts/prodbigdataserviceac.keytab" --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf" --jars "/Datalake/WD/spark_jars/ojdbc6-11.2.0.3.jar,$HOME/jars/kudu-spark2_2.11-1.10.0-cdh6.3.3.jar,$HOME/jars/hive-exec-2.1.1-cdh6.3.3.jar,$HOME/jars/scalaj-http_2.11-2.4.1.jar" --driver-class-path /data/WD/scripts/CRM_Scripts/ojdbc6-11.2.0.3.jar --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf" -i /data/WD/scripts/CRM_Scripts/ETL.scala