
Function Name - GetItem

-- Lab - User Defined Functions

function main(flowlog,index) {
    var Items = flowlog.split(',');
    return Items[index];
}


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



SELECT time,UDF.GetItem(flowstring,0) AS SourceIP,
UDF.GetItem(flowstring,2) AS SourcePort, 
UDF.GetItem(flowstring,5) AS Direction, 
UDF.GetItem(flowstring,6) AS Action
FROM stage5



