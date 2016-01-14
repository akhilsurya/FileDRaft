# akhilsurya

This is a simple file server with a read/write interface. There are two features that this file system has that traditional file systems don’t. The first is that each file has a version and the API supports a “compare and swap” operation based on the version. The second is that files can optionally expire after some time.
References : Go Documentation on golang.org
