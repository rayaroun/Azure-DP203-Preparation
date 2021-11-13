
-- Azure Synapse - Result set caching
-- First check if caching is set on the database

SELECT name, is_result_set_caching_on
FROM sys.databases;

-- Enable Query store

ALTER DATABASE newpool
SET QUERY_STORE = ON;

-- Set the context to the master database and then enable cache

ALTER DATABASE newpool
SET RESULT_SET_CACHING ON;

SELECT request_id, command, result_cache_hit FROM sys.dm_pdw_exec_requests
WHERE request_id = 'QID17612'

SELECT request_id, command, result_cache_hit FROM sys.dm_pdw_exec_requests
WHERE request_id = 'QID17614'