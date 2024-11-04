-- Load input file from HDFS
inputFile = LOAD 'hdfs:///user/pshirisha/pigText.txt' AS (line);
-- Tokeize each word in the file (Map)
words = FOREACH inputFile GENERATE FLATTEN(TOKENIZE(line)) AS word;
-- Combine the words from the above stage
grpd = GROUP words BY word;
-- Count the occurence of each word (Reduce)
cntd = FOREACH grpd GENERATE $0, COUNT($1) AS WordCount;
-- delete the output folder
rmf hdfs:///user/pshirisha/Pigresults;
-- Store the result in HDFS
STORE cntd INTO 'hdfs:///user/pshirisha/Pigresults';
