CREATE TABLE IF NOT EXISTS `node_updates` ( `id` integer PRIMARY KEY,   
`type` text NOT NULL, 
`path`,
`broker` integer,
`timestamp`  long NOT NULL);
