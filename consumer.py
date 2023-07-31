from kafka import KafkaConsumer
import boto3
from json import loads
from io import StringIO
from cred import access_key,secret_key

### Consumer obj creation..

consumer = KafkaConsumer(
    'cap',
    bootstrap_servers=['public ip of an EC2:9092'],
    value_deserializer=lambda x: loads(x.decode('utf-8')))


### Creating Connection With S3...

s3 = boto3.resource('s3',
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key,
                    )

### Uploading Consumer Data to S3 Bucket..

for count, i in enumerate(consumer):
    csv_buffer = StringIO()
    csv_buffer.write(str(i.value))
    filename = "stock_market_{}.json".format(count)
    s3.Object("kafka-stock-market-proj-shiv",filename).put(Body=csv_buffer.getvalue())
