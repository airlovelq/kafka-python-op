from kafka import KafkaConsumer
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
import time
from multiprocessing import Process
def test_kafka_consumer(i):
    consumer = KafkaConsumer('kkll3', bootstrap_servers='localhost:9092',auto_offset_reset='earliest',group_id='123', partition_assignment_strategy=[RoundRobinPartitionAssignor])
# consumer2 = KafkaConsumer('kkll3', bootstrap_servers='localhost:9092',auto_offset_reset='earliest',group_id='123', partition_assignment_strategy=[RoundRobinPartitionAssignor])
    time.sleep(3)
    for msg in consumer:
        recv = "%s:%d:%d: key=%s value=%s" % (msg.topic, msg.partition, msg.offset, msg.key, msg.value)
        consumer.commit_async()
        print(recv)
        ps = consumer.assignment()
        print(ps)
        print(i)
        time.sleep(5)

if __name__ == '__main__':
    j1 = Process(target=test_kafka_consumer,args=(1,))
    j2 = Process(target=test_kafka_consumer,args=(2,))
    j3 = Process(target=test_kafka_consumer,args=(3,))
    j1.start()
    j2.start()
    j3.start()
    j3.join()
    j2.join()
    j1.join()
# msg = next(consumer)
# recv = "%s:%d:%d: key=%s value=%s" % (msg.topic, msg.partition, msg.offset, msg.key, msg.value)
# print(recv)
# consumer.commit()
# ps = consumer.partitions_for_topic('kkll3')
# ps2 = consumer2.partitions_for_topic('kkll3')
# msg = next(consumer2)
# recv = "%s:%d:%d: key=%s value=%s" % (msg.topic, msg.partition, msg.offset, msg.key, msg.value)
# print(recv)
# msg = next(consumer)
# recv = "%s:%d:%d: key=%s value=%s" % (msg.topic, msg.partition, msg.offset, msg.key, msg.value)
# print(recv)
# consumer.commit_async()
# msg = consumer.__next__()
# recv = "%s:%d:%d: key=%s value=%s" % (msg.topic, msg.partition, msg.offset, msg.key, msg.value)
# print(recv)