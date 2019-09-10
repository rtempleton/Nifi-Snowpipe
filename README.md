# Nifi-Snowpipe
Apache Nifi processors for interacting with Snowpipe REST service


## SnowpipeIngest ([Data File Ingestion](https://docs.snowflake.net/manuals/user-guide/data-load-snowpipe-rest-apis.html#snowpipe-rest-api))
This processor is designed to invoke the REST endpoint responsible for ingesting a single given staged file. 

Use the SnowpipeIngest processor in a workflow following the PostS3Object or PutAzureBlobStorage processors

![snowpipe1.png](https://github.com/rtempleton/Nifi-Snowpipe/blob/master/img/snowpipe1.png)

**Configuration**</br>
- Specify your Snowflake region from the drop down list
- Provide your account, user and FULLY QUALIFIED pipe name
- Cut/Paste the contents of the [unencrypted private key](https://docs.snowflake.net/manuals/user-guide/data-load-snowpipe-rest-gs.html#using-key-pair-authentication) used to configure the Snowpipe service
- Use the `${filename}` Expression language reference to the file previously uploaded

![snowpipe2.png](https://github.com/rtempleton/Nifi-Snowpipe/blob/master/img/snowpipe2.png)

## SnowpipeHistoryRange ([Load History Reports](https://docs.snowflake.net/manuals/user-guide/data-load-snowpipe-rest-apis.html#endpoint-loadhistoryscan))
This processor calls the Snowpipe service to get back a list of processed files in the last N minutes. Schedule this processor to run in a reasonable amount of time with respect to the number of trailing minutes configured. Example: Run every 5 minutes polling for the list of files processed in the last 10 minutes. Evaluate the status of each returned record to take appropriate action in your workflow.

![snowpipe3.png](https://github.com/rtempleton/Nifi-Snowpipe/blob/master/img/snowpipe3.png)

**Configuration**</br>
The same configuration settings from the SnowpipeIngest processor plus the number of trailing minutes to search for.  

![snowpipe4.png](https://github.com/rtempleton/Nifi-Snowpipe/blob/master/img/snowpipe4.png)

**Note:** </br>
These processors have a dependency on the [snowflakedb/snowflake-ingest-java](https://github.com/snowflakedb/snowflake-ingest-java) github repo, however there is a an awaiting pull request for an outstanding bug. Until this request is merged, use the updated code repo [here](https://github.com/rtempleton/snowflake-ingest-java)

Lastly, you can pull down a binary build of the NAR [here](https://www.dropbox.com/s/nnw52xecmxim5s9/nifi-Snowpipe-nar-1.0-SNAPSHOT.nar?dl=0). This is ready to use, just drop it into your appropriate lib directory and restart Nifi.
