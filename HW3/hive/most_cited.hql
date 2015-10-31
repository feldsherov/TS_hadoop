!hadoop fs -rm /data/patents/apat63_99.txt most_cited_1990/hive/apat/apat63_99.txt;
!hadoop fs -rm /data/patents/cite75_99.txt most_cited_1990/hive/cited/cite75_99.txt;
!hadoop fs -cp /data/patents/apat63_99.txt most_cited_1990/hive/apat/apat63_99.txt;
!hadoop fs -cp /data/patents/cite75_99.txt most_cited_1990/hive/cited/cite75_99.txt;

use s_feldsherov;

DROP TABLE IF EXISTS patents;

CREATE EXTERNAL TABLE patents 
(pat_id BIGINT,
year BIGINT, 
GDATE STRING,
APPYEAR STRING,
COUNTRY STRING,
POSTATE STRING,
ASSIGNEE STRING,
ASSCODE STRING,
CLAIMS STRING,
NCLASS STRING,
CAT STRING,
SUBCAT STRING,
CMADE STRING,
CRECEIVE STRING,
RATIOCIT STRING,
GENERAL STRING,
ORIGINAL STRING,
FWDAPLAG STRING, 
BCKGTLAG STRING, 
SELFCTUB STRING, 
SELFCTLB STRING,
SECDUPBD STRING,
SECDLWBD STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/s.feldsherov/most_cited_1990/hive/apat'
tblproperties ("skip.header.line.count"="1");


DROP TABLE IF EXISTS cite;

CREATE EXTERNAL TABLE cite 
(citing BIGINT,
cited BIGINT) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/s.feldsherov/most_cited_1990/hive/cited'
tblproperties ("skip.header.line.count"="1");


SELECT 
    c.cited, " ",
    pciting.year, " ",
    pcited.COUNTRY, " ",
    COUNT(*) as cnt, " "
FROM 
    patents pciting INNER JOIN cite c ON 
    (pciting.pat_id = c.citing)
    INNER JOIN patents pcited ON
    (pcited.pat_id = c.cited)
WHERE pciting.year = 1990 AND pcited.COUNTRY = "\"US\""
GROUP BY c.cited, pciting.year, pcited.COUNTRY
ORDER BY cnt DESC
LIMIT 10;