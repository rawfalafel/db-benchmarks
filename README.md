Benchmarks for 3 KV data engines: LevelDB, Badger, Bolt. 

**Random Write, 1000 entries**
* LevelDB: 4.85s
* Badger: 4.80s
* Bolt: 5.32s

**Space utilization, 1000 entries**
* LevelDB: 380k
* Badger: 488k
* Bolt: 572k

**Random Read, 1M entries**
* LevelDB: 13.81s
* Badger: 9.08s
* Bolt: 3.45s

**Batch Write, 128k entries**
* LevelDB: 771ms
* Badger: 615ms
* Bolt: 4.54s

_Notes_
* All benchmarks use 32 byte random keys with 300 byte random values. Results were on an Intel i5 7200U with 8GB RAM
* For random write benchmarks, each write operation was configured to write to the hard drive. Worth noting that Bolt was the only database out of the 3 where writes were safe by default. 
* For batch write benchmarks, leveldb and badger provide an API that can efficiently batch using a single goroutine. Bolt's batch API requires each batch operation to be executed in a separate goroutine, and for each batch operation to be idempotent. Some tweaking was required to find the right balance between goroutines spawned and operations per batch.
