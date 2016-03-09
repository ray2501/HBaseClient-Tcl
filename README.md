
HBaseClient
=====

There are various ways to access and interact with [Apache HBase] (http://hbase.apache.org). 
The Java API provides the most functionality.
 
If users want to use HBase without Java, there are two main approaches for doing that: 
One is the Thrift interface, the other way to access HBase is using the REST interface.

This extension is a HBase REST Client Library for [Tcl] (http://tcl.tk).
The library consists of a single [Tcl Module] (http://tcl.tk/man/tcl8.6/TclCmd/tm.htm#M9) file.

HBaseClient is using Tcl built-in package http to send request to Apache HBase REST server and get response.

I only test this extension on Apache HBase standalone mode (Apache HBase 1.1.3 and Zookeeper 3.4.8,
no security settings).

User needs to start Aapache HBase REST Server daemon.

This extension needs Tcl 8.6 and tdom.


Interface
=====

The library has 1 TclOO class, HBaseClient.


Example
=====

Get HBase version:

    package require HBaseClient
    set myhbase [HBaseClient new http://localhost:8080]

    $myhbase version

List current tables, now the list is empty:

    $myhbase listTable

Then create a table:

    $myhbase createTable "test" "cf"

List current tables:

    $myhbase listTable

Put some values to test:

    $myhbase putValue "test" "row1" "cf" "a" "value1"
    $myhbase putValue "test" "row2" "cf" "b" "value2"
    $myhbase putValue "test" "row3" "cf" "c" "value3"

Get the values:

    $myhbase getValue "test" "row1"
    $myhbase getValue "test" "row2"
    $myhbase getValue "test" "row3"

Try to delete a value:

    $myhbase deleteValue "test" "row1" "cf" "a"


OK, now query again:

    $myhbase getValue "test" "row1"


User should get "Not found" response. In last step, drop the table:

    $myhbase deleteTable "test"
