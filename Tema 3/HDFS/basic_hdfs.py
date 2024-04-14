import csv
import os
from pywebhdfs.webhdfs import PyWebHdfsClient
from pprint import pprint
hdfs = PyWebHdfsClient(host='10.2.14.251',port='9870', user_name='root')
content=""
with open("Tema 3/Hadoop/data/train.csv", 'r') as file:
            content = file.read()

#HDFS
my_file = '/data/claims.csv'
hdfs.create_file(my_file, content)

#Posterior, tras ejecutar el job.
data=hdfs.read_file("user/root/nulos/part-m-00000")
with open("result.csv", 'w') as file:
    file.write(data)

