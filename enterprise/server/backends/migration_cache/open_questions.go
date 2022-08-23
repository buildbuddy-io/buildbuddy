package migration_cache

/*

Suggested impl: Only copy data from src -> dest when reading from src cache.
	Other than small performance issues (destination cache will have to rebuild a target if it's not cached), are there
	any problems if we don't copy all the data from the source -> dest (if a file is never read from the src db, it will
	never get copied to the dest)?

*/
