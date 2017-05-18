#!/usr/bin/env bash
mv pom.xml pom.xml.bak
cp pom.xml.shade pom.xml 
mvn clean
mvn package
mkdir target/ytk-mp4j
mkdir target/ytk-mp4j/lib
mkdir target/ytk-mp4j/log
cp -r bin target/ytk-mp4j
cp -r config target/ytk-mp4j
cp -r log/* target/target/ytk-mp4j/log
cp target/ytk-mp4j.jar  target/ytk-mp4j/lib
cd target
zip -r ytk-mp4j.zip ytk-mp4j
cd ..
mv pom.xml.bak pom.xml
