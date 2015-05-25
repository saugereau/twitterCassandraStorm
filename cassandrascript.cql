CREATE KEYSPACE tweetexp WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '2'}

CREATE TABLE tweetexp.tweets (
    id bigint PRIMARY KEY,
    createdat timestamp,
    lang text,
    source text,
    textt text,
    userdescription text,
    userlang text,
    userlocation text,
    username text,
    userscreenname text,
    usertimezone text,
    userurl text
)
