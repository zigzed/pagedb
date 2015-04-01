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

4.  iterator

	iterator: key size: 8Bytes, value size: 4Bytes, 1000000 records in 0.09seconds<br />


feature
=======
1.  high append performance
2.  acceptable query performance
3.  support duplicated items
4.  support prefix search
5.  support custom defined compare function (use memcmp default)

limitation
===========
1.  fixed length key & value (designed for index)
2.  not thread-safe yet
3.  no compress supported

