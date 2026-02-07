
HBaseClient
=====

There are various ways to access and interact with [Apache HBase](https://hbase.apache.org). 
The Java API provides the most functionality.
 
If users want to use HBase without Java, there are two main approaches for doing that: 
One is the Thrift interface, the other way to access HBase is using the
[REST](https://hbase.apache.org/book.html#_rest) interface.

This extension is a HBase REST Client Library for [Tcl](https://www.tcl-lang.org/).
The library consists of a single [Tcl Module](https://www.tcl-lang.org/man/tcl8.6/TclCmd/tm.htm) file.

HBaseClient is using Tcl built-in package http to send request to Apache HBase REST server and get response.

I only test this extension on Apache HBase standalone mode (Apache HBase 2.6.4, Zookeeper 3.9.4
and OpenJDK 21, no security settings).

User needs to start Aapache HBase REST Server daemon.

This extension needs Tcl 8.6, tdom and tcllib base64 package.


Interface
=====

The library has 1 TclOO class, HBaseClient.


Example
=====

## A simple example

Notice: The REST interface default port is 8080. However, in Apache ZooKeeper
versions 3.5.0 and later, port 8080 is the default port for the built-in
AdminServer, which provides an HTTP interface for command access.
So I pick up HBase REST interface port to 8090.

Get HBase version:

    package require HBaseClient
    set myhbase [HBaseClient new http://localhost:8090]

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

Do a stateless scan for our table:

    $myhbase scanRow "test"

Get the values:

    $myhbase getValue "test" "row1"
    $myhbase getValue "test" "row2"
    $myhbase getValue "test" "row3"

Or more detail parameters:

    $myhbase getValue "test" "row1" "cf" "a"

Try to delete a value:

    $myhbase deleteValue "test" "row1" "cf" "a"

Or you can try to delete more values (delete an entire row):

    $myhbase deleteValue "test" "row1"

OK, now query again:

    $myhbase getValue "test" "row1"

User should get "Not found" response. In the last step, drop the table:

    $myhbase deleteTable "test"

## HTTPS support

If user enables HTTPS support, below is an example:

    package require HBaseClient
    set myhbase [HBaseClient new https://localhost:8090 1]

Please notice, I use [TLS extension](http://core.tcl.tk/tcltls/index) to add https support.
So https support needs TLS extension.
