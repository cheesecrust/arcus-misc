ACP-JAVA: Arcus Java Client Performance benchmark program

Compile
-------
    mvn install
    mvn compile

Run
---
    mvn exec:java -Dexec.args="-config scenario/config-simple-decinc.txt"

How it works
------------

The  thread creates one or more ArcusClientPool's, and then creates
a number of worker threads.  Each worker thread executes the test code
specified in the config file (client_profile).

There is a primitive rate control.  Each worker thread sleeps a number of
milliseconds between commands to approximate the rate specified in the
config file.

How we use it
-------------

Mostly used to test Arcus 1.7's replication code.

License
-------

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
