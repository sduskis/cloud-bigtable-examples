This is a sample of Java and python code that writes messages to Cloud IoT via mqtt as per [this tutorial](https://cloud.google.com/iot/docs/protocol_bridge_guide#mqtt_client_samples), with modifications found in src/main/python. Also, it assumes a running OpenTSDB instance against Cloud Bigtable.

I followed [this](http://opentsdb.net/docs/build/html/user_guide/backends/bigtable.html) tutorial to install OpenTSDB.  This is a more fully featured integration found [here](https://cloud.google.com/solutions/opentsdb-cloud-platform).

I created the tables manually via the script found in cbt_table_creation_script.txt.
