import kafka
from kafka import KafkaProducer 
from kafka.errors import KafkaError 
from kafka.client import KafkaClient
from kafka import KafkaAdminClient
from kafka.admin.new_topic import NewTopic

admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
client = KafkaClient(hosts='localhost:9092')
# admin.create_topics([NewTopic(name='kkll3', num_partitions=3, replication_factor=1)])
producer = KafkaProducer(bootstrap_servers='localhost:9092') # Asynchronous by default 
#future = producer.send('my-topic2', b'raw_bytes')
for i in range(10):
    #time.sleep(1)
    producer.send('kkll3', key='foo{}'.format(i+1).encode(encoding='utf-8'), value='{}'.format(i+80).encode('utf-8'),partition=0)
    producer.flush()

for i in range(10):
    #time.sleep(1)
    producer.send('kkll3', key='foo{}'.format(i+1).encode(encoding='utf-8'), value='{}'.format(i+80).encode('utf-8'),partition=1)
    producer.flush()


for i in range(10):
    #time.sleep(1)
    producer.send('kkll3', key='foo{}'.format(i+1).encode(encoding='utf-8'), value='{}'.format(i+80).encode('utf-8'),partition=2)
    producer.flush()