from time import sleep
from json import dumps, encoder
from json import loads
import io
import kafka
import avro
#import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

#logging.basicConfig(level=logging.DEBUG)

file = "scadastatus.avsc"
schema = avro.schema.parse(open(file ,"rb").read())
writer = avro.io.DatumWriter(schema)
reader = avro.io.DatumReader(schema)
bytes_writer = io.BytesIO()
encoder = avro.io.BinaryEncoder(bytes_writer)

producer = kafka.KafkaProducer(bootstrap_servers=['10.0.0.4:9092']
                         )

writer.write({"id": "15986c5f-e164-4b3d-8a7a-e959feda71c9", "name": "AVC.DK1.IDU.AVC9", "value": 20, "quality1":10, "quality2": 20, "time": 10},encoder)
raw_bytes = bytes_writer.getvalue()

print(raw_bytes)
try:
 producer.send('scada-status',raw_bytes)
 producer.flush()
except:
 print("error")   

bytes_reader = io.BytesIO(raw_bytes)
decoder = avro.io.BinaryDecoder(bytes_reader)  
print(reader.read(decoder))