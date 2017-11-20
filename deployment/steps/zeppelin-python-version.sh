sudo sed -i -e 's/"zeppelin.python": "python",/"zeppelin.python": "python3",/g' /etc/zeppelin/conf/interpreter.json
sudo sed -i -e 's/"zeppelin.pyspark.python": "python",/"zeppelin.pyspark.python": "python3",/g' /etc/zeppelin/conf/interpreter.json
sudo sed -i -e 's/"perNoteProcess": false,/"perNoteProcess": true,/g' /etc/zeppelin/conf/interpreter.json

sudo /usr/lib/zeppelin/bin/zeppelin-daemon.sh stop

