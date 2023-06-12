DROP TABLE IF EXISTS student_list;

CREATE EXTERNAL TABLE IF NOT EXISTS student_list (
    num STRING,
    name STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
   	"separatorChar" = ",",
   	"quoteChar"     = "\""
    )  
    STORED AS TEXTFILE
    LOCATION '/user/school/student/'
    tblproperties ("skip.header.line.count"="1"
    );