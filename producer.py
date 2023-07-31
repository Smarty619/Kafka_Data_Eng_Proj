### Importing required modules
import pandas as pd
from kafka import KafkaProducer
from time import sleep
from json import dumps

### Creating Producer Object

producer = KafkaProducer(bootstrap_servers=['public ip of EC2:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))


df=pd.read_csv('indexProcessed.csv')

### Simulating the Data into RealTime

while True:
   dic=df.sample(1).to_dict(orient='records')[0]
   producer.send('cap',value=dic)
   sleep(1)
