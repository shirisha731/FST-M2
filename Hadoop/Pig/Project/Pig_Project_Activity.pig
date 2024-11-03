-- Load input file from HDFS
inputFile1 = LOAD 'hdfs:///user/Project_Activity/episodeIV_dialogues.txt' USING PigStorage('\t') AS (Name:chararray, Dialogue:chararray);
inputFile2 = LOAD 'hdfs:///user/Project_Activity/episodeV_dialogues.txt' USING PigStorage('\t') AS (Name:chararray, Dialogue:chararray);
inputFile3 = LOAD 'hdfs:///user/Project_Activity/episodeVI_dialogues.txt' USING PigStorage('\t') AS (Name:chararray, Dialogue:chararray);
summationFile = UNION inputFile1, inputFile2, inputFile3;
GroupByName = GROUP summationFile BY Name;
CountByName = FOREACH GroupByName GENERATE $0 AS NAME, COUNT($1) AS Lines_Spoken;
orderby = ORDER CountByName BY Lines_Spoken DESC;
STORE orderby INTO 'hdfs:///user/Project_Activity/Activity_1' USING PigStorage('\t');