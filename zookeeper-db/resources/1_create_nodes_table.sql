CREATE TABLE IF NOT EXISTS `nodes` (  `pk` integer PRIMARY KEY, `fk` integer NULL,  
`name` text NOT NULL, 
`data` BLOB,
`version` integer NOT NULL DEFAULT 0,
`cversion` integer NOT NULL DEFAULT 0,
`timestamp`  long NOT NULL,
`mode` text NOT NULL default 'PERSISTENT', 
`session` integer,
FOREIGN KEY (fk) REFERENCES nodes(pk), UNIQUE(name) );
INSERT INTO nodes (name,timestamp) VALUES ("/",'1499865299000');
