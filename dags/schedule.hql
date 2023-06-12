DROP TABLE IF EXISTS time_schedule;

CREATE EXTERNAL TABLE IF NOT EXISTS time_schedule (
    day STRING,
    time STRING,
    schedule STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
   	"separatorChar" = ",",
   	"quoteChar"     = "\""
    )  
    STORED AS TEXTFILE
    LOCATION '/user/school/schedule/'
    tblproperties ("skip.header.line.count"="1"
    );