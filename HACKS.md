### Duplicate Detector

#### To Run
```
sharkspotter --duplicates -D -T -m 1 -M 2 -d east.joyent.us -l debug > sharkspotter.log &
```

#### Check Status
1. In the log you will see the DB name:
	```
	[rui@db2be7c4-5906-e592-b520-ae6d2ee55534 ~/git/rust-sharkspotter]$ tail -f
	sharkspotter.log
	Creating database 20200914T182228 <<<<<<<<<<<<<<<<<<<
	Creating stub and duplicate tables
	```

2. Check the status of the run by looking at the two tables:
	```
	[rui@db2be7c4-5906-e592-b520-ae6d2ee55534 ~]$ psql -U postgres 20200914T182228
	psql (12.1)
	Type "help" for help.

	20200914T182228=# select count(*) from mantastubs;
	 count 
	-------
	  2674
	(1 row)
	```
	```
	20200914T182228=# select count(*) from mantaduplicates;
	 count 
	-------
		 0
	(1 row)

	```
The `mantastubs` table holds a small amount of information about each object.  Look in this table to find duplicate entries.
The `mantaduplicates` table holds the metadata entry of a duplicate object.  This is intended as a backup in the event that we delete the incorrect information.


### Copies > 2
sharkspotter -C 2 -D -T -m 1 -M 2 -d east.joyent.us -l debug > sharkspotter.log
&
