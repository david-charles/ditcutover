CREATE TABLE sp_stats (pk SERIAL PRIMARY KEY,
						process VARCHAR NOT NULL,
						params VARCHAR,
						step VARCHAR,
						rowcount INTEGER);
