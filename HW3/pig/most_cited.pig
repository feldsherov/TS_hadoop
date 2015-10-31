pre_patents = LOAD 'most_cited_1990/pig/apat63_99.txt' USING PigStorage(',') AS 
(id:INT, year:INT, GDATE:chararray,APPYEAR:chararray,country:chararray,POSTATE:chararray,ASSIGNEE:chararray, ASSCODE:chararray, CLAIMS:chararray, NCLASS:chararray, CAT:chararray, SUBCAT:chararray, CMADE:chararray, CRECEIVE:chararray,RATIOCIT:chararray, GENERAL:chararray,ORIGINAL:chararray, FWDAPLAG:chararray, BCKGTLAG:chararray,SELFCTUB:chararray, SELFCTLB:chararray, SECDUPBD:chararray, SECDLWBD:chararray);
patents = FOREACH pre_patents GENERATE $0, $1, $4;

cite = LOAD 'most_cited_1990/pig/cite75_99.txt' USING PigStorage(',') AS (citing:INT, cited:INT);
joined = JOIN cite by citing, patents by id;
filtered = FILTER joined by patents::year == 1990;
grouped = GROUP filtered BY cite::cited;
counts = FOREACH grouped GENERATE group, COUNT_STAR(filtered);

joined_2 = JOIN counts by $0, patents by id;
result = FILTER joined_2 by patents::country == '"US"';
ordered_result = ORDER result BY $1 DESC;
to_print = LIMIT ordered_result 10;
DUMP to_print;