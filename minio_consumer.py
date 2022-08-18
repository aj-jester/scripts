from minio import Minio
import urllib3

from kafka import KafkaConsumer
import json

# Convenient dict for basic config
config = {
  "dest_bucket":    "processed", # This will be auto created
  "minio_endpoint": "minio.tenant-lite.svc.cluster.local",
  "minio_username": "minio",
  "minio_password": "minio123",
  "kafka_servers":  "10.244.1.5:29092",
  "kafka_topic":    "my-topic", # This needs to be created manually
}

# Since we are using self-signed certs we need to disable TLS verification
http_client = urllib3.PoolManager(cert_reqs='CERT_NONE')
urllib3.disable_warnings()

# Initialize MinIO client
minio_client = Minio(config["minio_endpoint"],
               secure=True,
               access_key=config["minio_username"],
               secret_key=config["minio_password"],
               http_client = http_client
               )

# Create destination bucket if it does not exist
if not minio_client.bucket_exists(config["dest_bucket"]):
  minio_client.make_bucket(config["dest_bucket"])
  print("Destination Bucket '%s' has been created" % (config["dest_bucket"]))

# Initialize kafka consumer
consumer = KafkaConsumer(
  bootstrap_servers=config["kafka_servers"],
  value_deserializer = lambda v: json.loads(v.decode('ascii'))
)

consumer.subscribe(topics=config["kafka_topic"])

try:
  print("Ctrl+C to stop Consumer\n")
  for message in consumer:
    message_from_topic = message.value

    request_type = message_from_topic["EventName"]
    bucket_name, object_path = message_from_topic["Key"].split("/", 1)

    # Only process the request is a new object is created via PUT
    if request_type == "s3:ObjectCreated:Put":
      minio_client.fget_object(bucket_name, object_path, object_path)
      
      print("- Doing some pseudo image resizing or ML processing on %s" % object_path)

      minio_client.fput_object(config["dest_bucket"], object_path, object_path)
      print("- Uploaded processed object '%s' to Destination Bucket '%s'" % (object_path, config["dest_bucket"]))
except KeyboardInterrupt:
  print("\nConsumer stopped.")