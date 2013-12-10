#java-redis-rdb#

resolve redis dump.rdb file

High Level Algorithm to parse RDB
==========

At a high level, the RDB file has the following structure
<pre><code>
----------------------------# RDB is a binary format. There are no new lines or spaces in the file.
52 45 44 49 53              # Magic String "REDIS"
30 30 30 33                 # RDB Version Number in big endian. In this case, version = 0003 = 3
----------------------------
FE 00                       # FE = code that indicates database selector. db number = 00
----------------------------# Key-Value pair starts
FD $unsigned int            # FD indicates "expiry time in seconds". After that, expiry time is read as a 4 byte unsigned int
$value-type                 # 1 byte flag indicating the type of value - set, map, sorted set etc.
$string-encoded-key         # The key, encoded as a redis string
$encoded-value              # The value. Encoding depends on $value-type
----------------------------
FC $unsigned long           # FC indicates "expiry time in ms". After that, expiry time is read as a 8 byte unsigned long
$value-type                 # 1 byte flag indicating the type of value - set, map, sorted set etc.
$string-encoded-key         # The key, encoded as a redis string
$encoded-value              # The value. Encoding depends on $value-type
----------------------------
$value-type                 # This key value pair doesn't have an expiry. $value_type guaranteed != to FD, FC, FE and FF
$string-encoded-key
$encoded-value
----------------------------
FE $length-encoding         # Previos db ends, next db starts. Database number read using length encoding.
----------------------------
...                         # Key value pairs for this database, additonal database
                            
FF                          ## End of RDB file indicator
8 byte checksum             ## CRC 64 checksum of the entire file.
</code></pre>