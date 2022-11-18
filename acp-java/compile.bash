#!/bin/bash

echo "Jar is at " $JARFILE
javac -Xlint:deprecation -classpath "$HOME/arcus-java-client/target/*" *.java
#javac *.java
