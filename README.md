Example usage:
```
./redis-mover --export -file=dump_test.json --redis=redis://:password@localhost:6379/1,2,3
```
This will export dbs 1, 2 and 3 to file dump_test.json. 

By running the following command the file will be imported into redis. (Database-number is same as at export)

```
./redis-mover --import -file=dump_test.json --redis=redis://:password@localhost:6379
```
