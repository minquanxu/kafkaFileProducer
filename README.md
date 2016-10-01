# kafkaFileProducer

This project demonstrates a quick way to ingest data to kafka. In my company we need to stream data from sql server to hadoop cluster, original kafka producer was writtern in C# using Misakai kafka lib. It can ingest data at 60 rows per second, which meets our requirement when system goes live. However, problem rises when back fill huge amount of historical data. Since we can not directly access production sql server, we get data from DR which DBA will setup DR in the morning and shut it down at 5 O'clock.

Based on this rate, it will need 78 days to finish ingestion. Apprarently it is not acceptable to our timeline. So started to explore new methods. Misakai lib does not expose a lot propties, so I use java. Initial version, I used regular send method, it performed just like Miskai lib, 60 rows/s. After some research and testing, I discovered that using callback and set ack to all, is fastest way to do ingestion. This methods can ingest close to 20,000 rows/s. 

To save DBA time and also allow it continuesly run ingestion over night, I save all data to my local file system first, then let producer read local files. With new producer, the ingestion finishes less than 24 hours.

<h2>To build</h2>
linux
download the project, in the project root, type
mvn clean package
uber-kafka-file-producer-0.0.1-SNAPSHOT.jar will be generated in target dir

windows
You can build it using eclipse or other IDE, export jar

Run
linuxBox> nohup java -Xms256m -Xmx1096m -jar uber-kafka-file-producer-0.0.1-SNAPSHOT.jar ./root_file_folder dir_list.txt
where 

File layout example
SurveyBackFillFiles
   sql_server_table1
     file1
     file2
     ...
   sql_server_table2
     file1
     file2
   ...

Issues and Solutions
1. run the producer in Windows, it does not handle internal chars properly, so I run producer in Linux (changing eclipse char setting in Windows did not solve the problem)
2. If copy files from Windows to unix, please run dos2unix, otherwise down stream program such as storm may have problem to process the data
