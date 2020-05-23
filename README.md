# Graph-Processing-on-Spark

Same task as graph processing using map reduce but here we have used spark and scala 

If you have Mac OS or Linux, make sure you have Java and Maven installed. If you have Windows 10, see project1 on how to install Ubuntu Shell. On Windows 10, you will need to set up your JAVA_PATH using: export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 (you can put this in your .bashrc file so you don't have to type it every time).


To install Spark and the project:

cd

wget https://archive.apache.org/dist/spark/spark-1.5.2/spark-1.5.2-bin-hadoop2.6.tgz

tar xfz spark-1.5.2-bin-hadoop2.6.tgz  

To compile and run the examples/src/main/scala/JoinSpark.scala example:

cd project4/examples

rm -rf output

mvn install

~/spark-1.5.2-bin-hadoop2.6/bin/spark-submit --class JoinSpark target/cse6331-spark-examples-0.1.jar e.txt d.txt output

To compile and run project4:

cd project4

mvn install

~/spark-1.5.2-bin-hadoop2.6/bin/spark-submit --class Graph target/cse6331-project4-0.1.jar small-graph.txt 
