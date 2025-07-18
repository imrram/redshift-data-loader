#!/bin/bash
javac -cp .:drivers/redshift-jdbc42-2.1.0.9.jar AmazonRedshift.java
java -cp .:drivers/redshift-jdbc42-2.1.0.9.jar AmazonRedshift