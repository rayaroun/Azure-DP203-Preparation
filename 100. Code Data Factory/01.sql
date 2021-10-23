-- Lab - Self-Hosted Runtime - Copy Activity

CREATE TABLE [serverlogs]
(
[remote_addr] varchar(20),
[time_local] varchar(100),
[request] varchar(200),
[status] int,
[bytes] int,
[remote_user] varchar(100),
[http_user_agent] varchar(500)
)
