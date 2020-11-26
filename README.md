Migration-Tool
==============

Tool to help migrate an Axon 1.x event store to the Axon 2.0 format

Before you start
----------------

Before you get started using this tool, you need to convert your codebase to use Axon 2. The migration tool needs a
JAR file with the events (in Axon 2 format) to validate the conversion.
Visit: http://www.axonframework.org/axon-2-migration-guide/

Setting up the tool
-------------------

First, download (or build) the migration tool. Extract the ZIP file to a location where you can easily find it again.

There are 3 directories and 3 files that may be of interest to you.
In the events directory, place the JAR containing the Axon 2 style Events. The tool uses these classes to detect the new field for the aggregate identifier.

In the upcasters directory, place the JAR that contains the (Axon 1 style) upcaster classes.

In the other_deps directory, you can place any JARS that your upcasters or events require. You can also place the database driver JAR in this directory.

Open the migration.properties file, and make sure the configuration is valid for your scenario. Most likely, you need to change the database settings.

In identifiers.properties, you can configure the field names that contain the aggregate identifier. The migration tool will try to autodetect these fields (unless it is switched off in the configuration) if they are not mentioned in this file.

In app-specific-context.xml, you must define the upcasters that you use. For each upcaster, provide a simple bean definition.

Running the tool
----------------

To run the tool, simply execute migrate.bat (windows) or migrate.sh (linux). The process starts right away.
While processing it keeps you updated on its progress. It is always possible to stop and restart the processor at any time.

Note that the migration tool will store the converted events in another table than the old events are stored in. Automatically generated tables may not have the necessary indexes. Therefore, add the necessary indexes before the while database is migrated. Altering a near empty table is generally much faster than altering one with lots of data.

Questions and help
------------------
If you need help, or have a question about this migration tool, you can post it on [AxonIQ's discussion platform](https://discuss.axoniq.io/)

License
-------
The migration tool is licensed under Apache 2 license:
http://www.apache.org/licenses/LICENSE-2.0.html
