# Overview
This is a work in progress!

Dependencies:
-  HBase 0.94.0 or later
-  thrift 0.8.0
-  google logging
-  google flags
-  gcc 4.6 or later, or some similarly C++11 compliant compiler
-  facebook's folly library (get the latest from github at
   https://github.com/facebook/folly/)

For now, see hbc.cpp and LiveClusterTesting.cpp for examples of use of
the API.

# Setting up your HBase environment

NHC currently requires HBase 0.94.0 or newer.  In addition, you should
configure your environment to enable the Region Server's embedded
thrift server.  Specifically, put the following in your hbase-site.xml file:

    <property>
      <name>hbase.regionserver.export.thrift</name>
      <value>true</value>
    </property>
    <property>
      <name>hbase.regionserver.thrift.server.type</name>
      <value>threadedselector</value>
    </property>
    <property>
      <name>hbase.regionserver.thrift.port</name>
      <value>9091</value>
    </property>

In a nutshell, this enables the in-region server thrift support,
changes to the 'threadedselector' server type (the most efficient and
high performance option), and asks it to listen to port 9091 (it will
otherwise listen to port 9090 which conflicts with the default thrift
port).

Restart your cluster, and you should now be able to test!  For example:

    ./hbc --zookeeper localhost:2181 tables

To test the API with the hbc utility (included in this distribution).
In fact...

# hbc: a command line tool for working with hbase!
hbc is a simple command line tool for use with an HBase cluster. The
tool is capable of getting and setting data, creating and dropping
tables, and a variety of other commonly useful tasks.

## Why not use hbase shell?

There is nothing wrong with the hbase shell; it's quite useful and
functional. But it is more complicated to use, requiring xml
configuration files, and presents a somewhat non-Unix interface. hbc,
on the other hand, requires one piece of configuration (the ZooKeeper
cell the HBase cluster uses), and is well suited for more Unix-like
interaction. It also is useful for testing the Native C++ Client and
more suited for scripting.


    Usage: hbc [params] command [command params]

    Note: all commands rely on either the --zookeeper option or the
    HBC_ZOOKEEPER environment variable, which contains the ZooKeeper
    instance where the HBase cluster stores its state.

    No ZooKeeper instance set via command line or environment

    Subcommands:
      Table/Cell inspection:
        hbc tables
        hbc schema <table>
        hbc regions <table>

      Data manipulation:
        hbc scan [ --start_row R ] [ --end_row R ] [ --column C ] [--num_scan_rows N] <table>
        hbc get <table> <row>
        hbc set <table> <row> column=valuue column=value ...
        hbc delete <table> <row> [column column ...]

      TAO operations:
        hbc taoput <table> <id1> <id2> <type> <payload>
        hbc taodelete <table> <id1> <id2> <type> [true|false]
        hbc taoget <table> <id1> <type>

      Schema Manipulation:
        hbc create <table> /path/to/schema
        hbc create <table> (schema read from stdin)
        hbc drop <table>
        hbc enable <table>
        hbc disable <table>
        hbc status <table>

## Some Sample Usage
    [doctor@gallifrey hbase] hbc --zookeeper zk1:2181,zk2:2181 tables
    Table: abc_test_table
    Table: chip_mysql_import
    ...


    [doctor@gallifrey hbase] hbc --zookeeper zk1:2181,zk2:2181 scan --num_scan_rows 2 --start_row 400 chip_mysql_import
    Row 400
      fields:id - 15
      fields:value - "row 400"
    Row 4001
      fields:id - 324
      fields:value - "row 4001"
    Row 401
      fields:id - 17
      fields:value - "row 401"


    [doctor@gallifrey hbase] hbc --zookeeper zk1:2181,zk2:2181 schema chip_mysql_import | tee /tmp/table.schema
    # Table schema for chip_mysql_import
    # Dumped on Wed Jul 13 11:38:11 2011

    <
      name: fields:
      maxVersions: 3
      compression: LZO
      inMemory: 0
      bloomFilterType: NONE
      bloomFilterVectorSize: 0
      bloomFilterNbHashes: 0
      blockCacheEnabled: 1
      timeToLive: -1
    >
    [doctor@gallifrey hbase] hbc --zookeeper zk1:2181,zk2:2181 create chip_mysql_import_new < /tmp/table.schema
    Create successful!

    [doctor@gallifrey hbase] hbc --zookeeper zk1:2181,zk2:2181 schema chip_mysql_import_new
    # Table schema for chip_mysql_import_new
    # Dumped on Wed Jul 13 11:38:39 2011

    <
      name: rowdata:
      maxVersions: 3
      compression: LZO
      inMemory: 0
      bloomFilterType: NONE
      bloomFilterVectorSize: 0
      bloomFilterNbHashes: 0
      blockCacheEnabled: 1
      timeToLive: -1
    >

    [doctor@gallifrey hbase] ~/bin/hbc --zookeeper zk1:2181,zk2:2181 drop chip_mysql_import_new
    Unable to drop table: org.apache.hadoop.hbase.TableNotDisabledException: chip_mysql_import_new
           at org.apache.hadoop.hbase.master.HMaster.checkTableModifiable(HMaster.java:917)
           at org.apache.hadoop.hbase.master.handler.TableEventHandler.<init>(TableEventHandler.java:54)
           at org.apache.hadoop.hbase.master.handler.DeleteTableHandler.<init>(DeleteTableHandler.java:42)
           at org.apache.hadoop.hbase.master.HMaster.deleteTable(HMaster.java:834)
           at sun.reflect.GeneratedMethodAccessor16.invoke(Unknown Source)
           at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:25)
           at java.lang.reflect.Method.invoke(Method.java:597)
           at org.apache.hadoop.hbase.ipc.HBaseRPC$Server.call(HBaseRPC.java:569)
           at org.apache.hadoop.hbase.ipc.HBaseServer$Handler.run(HBaseServer.java:1173)

    [doctor@gallifrey hbase] ~/bin/hbc --zookeeper zk1:2181,zk2:2181 disable chip_mysql_import_new
    Table disabled.
    [doctor@gallifrey hbase] ~/bin/hbc --zookeeper zk1:2181,zk2:2181 drop chip_mysql_import_new
    Table deleted.
