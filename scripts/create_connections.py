from airflow import settings
from airflow.models import Connection
from airflow.models import Variable

aws = Connection(
    conn_id='aws_credentials',
    conn_type='aws',
    login='<access key id>',
    password='<secret key>',
)  # create a connection object

redshift = Connection(
    conn_id='redshift',
    conn_type='postgres',
    host='<host>',
    schema='<database name>',
    login='<database user>',
    password='<database password>',
    port='5439'
)

session = settings.Session()  # get the session
session.add(aws)
session.add(redshift)
session.commit()  # it will insert the connection object programmatically.
