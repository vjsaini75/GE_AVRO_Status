from json import loads
import io
import kafka
from kafka import KafkaConsumer
import avro
#import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

file = "scadastatus.avsc"
schema = avro.schema.parse(open(file ,"rb").read())
reader = avro.io.DatumReader(schema)

consumer = KafkaConsumer(
    'scada-status',
     bootstrap_servers=['10.0.0.4:9092'],
     group_id = 'vijay08',
     auto_offset_reset='earliest')


#bytes_data =  bytes_reader.getvalue()
print(consumer)
for message in consumer:
    message = message.value
#    message = message.value
#    print(message)
    bytes_reader = io.BytesIO(message)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    print(reader.read(decoder))
    reader.read(decoder).id
    consumer.commit()