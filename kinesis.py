# -*- coding: utf-8 -*

import requests
from bs4 import BeautifulSoup
import sys
import avro.schema
import io, random
from avro.io import DatumWriter
reload(sys)
sys.setdefaultencoding('utf8')
from kinesis_producer import KinesisProducer

config = dict(
    aws_region='eu-west-3',
    buffer_size_limit=100000,
    buffer_time_limit=0.2,
    kinesis_concurrency=1,
    kinesis_max_retries=10,
    record_delimiter='\n',
    stream_name='recomender',
    )

k = KinesisProducer(config=config)
records = [
{"marque":"TOM FORD","produit":"TRANSLUCENT FINISHING POWDER","teint":"ALABASTER NUDE","color":[255, 230, 207],"price":"79,00$","img_link":"n/a","link":"https://www.tomford.com/translucent-finishing-powder/T0T7.html?dwvar_T0T7_color=ALABASTERNUDE&cgid=beauty-face-powder#start=1","city":"New York","country":"UNITED STATES","since":"2008"},
{"marque":"TOM FORD","produit":"TRANSLUCENT FINISHING POWDER","teint":"IVORY FAWN","color":[238, 200, 176],"price":"79,00$","img_link":"n/a","link":"https://www.tomford.com/translucent-finishing-powder/T0T7.html?dwvar_T0T7_color=ALABASTERNUDE&cgid=beauty-face-powder#start=1","city":"New York","country":"UNITED STATES","since":"2008"},
{"marque":"TOM FORD","produit":"TRANSLUCENT FINISHING POWDER","teint":"SAHARA DUSK","color":[196, 142, 119],"price":"79,00$","img_link":"n/a","link":"https://www.tomford.com/translucent-finishing-powder/T0T7.html?dwvar_T0T7_color=ALABASTERNUDE&cgid=beauty-face-powder#start=1","city":"New York","country":"UNITED STATES","since":"2008"},
{"marque":"TOM FORD","produit":"TRANSLUCENT FINISHING POWDER","teint":"SABLE VOILE","color":[179, 118, 88],"price":"79,00$","img_link":"n/a","link":"https://www.tomford.com/translucent-finishing-powder/T0T7.html?dwvar_T0T7_color=ALABASTERNUDE&cgid=beauty-face-powder#start=1","city":"New York","country":"UNITED STATES","since":"2008"},
{"marque":"TOM FORD","produit":"ILLUMINATING POWDER","teint":"TRANSLUCENT","color":[255, 243, 226],"price":"79,00$","img_link":"n/a","link":"https://www.tomford.com/translucent-finishing-powder/T0T7.html?dwvar_T0T7_color=ALABASTERNUDE&cgid=beauty-face-powder#start=1","city":"New York","country":"UNITED STATES","since":"2008"},
{"marque":"TOM FORD","produit":"ILLUMINATING POWDER","teint":"TRANSLUCENT PINK","color":[248, 234, 234],"price":"79,00$","img_link":"n/a","link":"https://www.tomford.com/translucent-finishing-powder/T0T7.html?dwvar_T0T7_color=ALABASTERNUDE&cgid=beauty-face-powder#start=1","city":"New York","country":"UNITED STATES","since":"2008"}
]
schema_path="user.avsc"
schema = avro.schema.parse(open(schema_path).read())
for record in records:
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(record, encoder)
    raw_bytes = bytes_writer.getvalue()
    k.send(raw_bytes)
    print record

k.close()
k.join()


from kinesis.consumer import KinesisConsumer

consumer = KinesisConsumer(stream_name='recomender', region_name='eu-west-3')
for message in consumer:
    print "Received message: {0}".format(message)
