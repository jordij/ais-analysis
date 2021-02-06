CREATE TABLE IF NOT EXISTS messages (
	id serial PRIMARY KEY,
	userid integer,
    messageid integer,
    utctimestamp timestamp,
    latitude float,
    longitude float,
    speed float
);
CREATE INDEX ON messages (UserID);
CREATE INDEX ON messages (UserID, UTCTimeStamp);