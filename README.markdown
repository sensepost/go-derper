# 1. Name

go-derper - Memcache hacking tool

# 2. Author

Marco Slaviero < marco(at)sensepost(dot)com >

# 3. License, version & release date

License : BSD  
Version : v1.0  
Release Date : 2010

# 4. Description

go-derper.rb is a tool for hacking memcached servers, released as part of our BlackHat USA. It uses elements of the memcached protocol to derive full lists of keys stored on the memcached server, and can therefore extract the contents of the cache.

In addition, it also supports basic searching of retrieved data via user-configurable regular expressions, fingerprinting of multiple caches, monitoring usage in caches as well as basic cache content manipulations such as value insertion, overwrites and deletion.

# 5. Usage

Extract contents of a cache (defaults to 10 keys per slab)
> ./go-derper.rb -l -s < hostname >

Extract contents of a cache, using 100 keys per slab
> ./go-derper.rb -l -K 100 -s < hostname >

Extract contents of a cache, using 100 keys per slab, print out values matching regexes found regexs.txt
> ./go-derper.rb -l -K 100 -s < hostname > -R regexs.txt

Write back into the cache, the value stored at output/run5-c4ecee795335e7ef662e661974699448
> ./go-derper.rb -w output/run5-c4ecee795335e7ef662e661974699448
When writing values into the cache, local paths needs to be resolved. Run go-derper from inside it's
own root.

Delete the value stored at output/run5-c4ecee795335e7ef662e661974699448
> ./go-derper.rb -d output/run5-c4ecee795335e7ef662e661974699448
When deleting values from the cache, local paths needs to be resolved. Run go-derper from inside it's
own root.

Pull stats from one cache:
> ./go-derper.rb -s < hostname > -S

Fingerprint multiple caches:
> ./go-derper.rb -f < host1 >,< host2 >,...,< hostn >

Fingerprint multiple caches stored in a file (one per line):
> ./go-derper.rb -F < file >

Monitor a single cache to watch changes:
> ./go-derper.rb -m -s < hostname >

Pull a single key:
> ./go-derper.rb -k < keyid > -s < hostname >

# 6. Requirements

- Ruby (Tested on 1.8.6)
- memcache-client gem (gem install memcache-client)
Note: we include a modified memcache.rb from memcache-client. Thus parts of this package
are subject to their BSD license. See memcache-client-license.txt
- disk space and bandwidth :)

# 7. Additional Resources

Blog, BlackHat Write-up: go-derper and mining memcaches - https://sensepost.com/blog/2010/blackhat-write-up-go-derper-and-mining-memcaches/
