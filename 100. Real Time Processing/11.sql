WITH
stage1 AS
(
SELECT
   Records.ArrayValue.time AS time,
   Records.ArrayValue.properties.flows AS flows
FROM nsg n
    CROSS APPLY GetArrayElements(n.records) AS Records 
),
stage2 AS
(
SELECT
time, GetArrayElement(flows,0) as flows
FROM stage1
),
stage3 AS
(
SELECT time,GetArrayElement(flows.flows,0) as flows
FROM stage2
),
stage4 AS
(
SELECT s.time,flowTuples.ArrayValue AS flowtuple
FROM stage3 s
CROSS APPLY GetArrayElements(s.flows.flowTuples) AS flowTuples 
),
stage5 AS
(
SELECT time,SUBSTRING(flowtuple,13,LEN(flowtuple)) AS flowstring
FROM stage4
)

SELECT 
time,SUBSTRING(flowstring,0,CHARINDEX(',',flowstring)) AS IPAddress,
SUBSTRING(flowstring,LEN(flowstring),LEN(flowstring)) AS Action
FROM stage5


=====================

Using Windowing functions


WITH
stage1 AS
(
SELECT
   Records.ArrayValue.time AS time,
   Records.ArrayValue.properties.flows AS flows
FROM nsg n
    CROSS APPLY GetArrayElements(n.records) AS Records 
),
stage2 AS
(
SELECT
time, GetArrayElement(flows,0) as flows
FROM stage1
),
stage3 AS
(
SELECT time,GetArrayElement(flows.flows,0) as flows
FROM stage2
),
stage4 AS
(
SELECT s.time,flowTuples.ArrayValue AS flowtuple
FROM stage3 s
CROSS APPLY GetArrayElements(s.flows.flowTuples) AS flowTuples 
),
stage5 AS
(
SELECT time,SUBSTRING(flowtuple,13,LEN(flowtuple)) AS flowstring
FROM stage4
),
finalstage AS
(
SELECT 
time,SUBSTRING(flowstring,0,CHARINDEX(',',flowstring)) AS IPAddress,
SUBSTRING(flowstring,LEN(flowstring),LEN(flowstring)) AS Action
FROM stage5
)

SELECT IPAddress,COUNT(*) AS Count
FROM finalstage
GROUP BY IPAddress,Action, TumblingWindow(second,30)
HAVING Action='D'

-- Using Hopping Window

SELECT IPAddress,COUNT(*) AS Count
FROM finalstage
GROUP BY IPAddress,Action, HopingWindow(second,30,10)
HAVING Action='D'

-- Using Sliging Window

SELECT IPAddress,COUNT(*) AS Count
FROM finalstage
GROUP BY IPAddress,Action, SlidingWindow(second,30)
HAVING Action='D' AND COUNT(*) > 5

CREATE TABLE NSG
(
IPAdress varchar(16),
Count int
)


