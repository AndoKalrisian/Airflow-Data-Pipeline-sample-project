# Data Pipeline Using Airflow

In this project I create a data pipeline that takes data from Amazon S3, stages it in a redshift database and then transforms the staged data into a star schema.  Along the way I have inserted data quality tests after the data is stored in redshift and then again when the data is stored in the star schema.

#### Dag Graph

![dag graph](/dag_graph.PNG "dag graph")

#### Star Schema

![schema](/star_schema.png "schema")

### Setup Notes

To get airflow up and running I used a dockerized version of airflow called [puckel/docker-airflow](https://github.com/puckel/docker-airflow) on my Windows 10 PC.  I used docker-compose-CeleryExecutor.yml as a template for my own build.

```requirements.txt file``` was added to install the boto3 and boto dependencies that are required to connect to AWS.  This is configured in the ```docker-compose.yml``` file.

Shared volumes were configured in the ```docker-compose.yml``` file for 'dags', 'plugins', and 'scripts'.

I also included a script to setup the aws_credentials and redshift connections ```scripts/create_connection.py```.  This should be run everytime you restart the airflow webserver.

#### Problems I ran into

When pulling from S3 I got errors related to time skew.  After some investigation I found that my docker container was using the wrong time and date.  This can be checked using the following command:
```
docker exec -it <docker-container-name> date
```
After some searching I found a good solution [here](https://thorsten-hans.com/docker-on-windows-fix-time-synchronization-issue).  I made a shortcut included in this repo called ```fix-docker-time-sync.ps1```.  This needs to be 'run as administrator'.  If you use this, you will need to change the path in the shortcut to match your system.




