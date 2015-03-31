# pagedb
a fast append only key value store. High append performance with acceptable query performance.

performance 
===========
1.  baseline (dd)

	dd if=/dev/zero of=./baseline.dat ibs=24 obs=4MB count=10000000 (80MB/s)<br />
	dd if=/dev/zero of=./baseline.dat ibs=24 obs=4MB count=10000000  conv=fdatasync (40MB/s)<br />

2.  sequence append

	insert: key size: 20Bytes, value size: 4Bytes, append 10000000 records (224MB) in 5seconds<br />
	query:  prefix search with 10 results: 0.002seconds~0.004seconds<br />

3.  random append

	insert: key size: 20Bytes, value size: 4Bytes, append 10000000 records (224MB) in 9seconds<br />
	query:  prefix search with 10 results: 0.002seconds~0.004seconds<br />
