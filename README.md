# akhilsurya

This is a consistent, replicated file system that uses the RAFT consensus protocol for log sharing.
- Read a particular file
- Write to it and get the current version
- Compare and Swap contents(CAS) by giving the correct(current) version number of the file
- Delete a particular file.
 
The files can be made to expire after a given amount of time specified during writes  or made to stay forever.
### Sample Usage

``` sh
$ go run server.go &

```
### The API
| Command  | Success Response | Error Response
|----------|-----|----------|
|read _filename_ \r\n| CONTENTS _version_ _numbytes_ _exptime remaining_\r\n <br> _content bytes_\r\n <br>| ERR_FILE_NOT_FOUND
|write _filename_ _numbytes_ [_exptime_]\r\n<br>_content bytes_\r\n| OK _version_\r\n| |
|cas _filename_ _version_ _numbytes_ [_exptime_]\r\n</br>_content bytes_\r\n| OK _version_\r\n | ERR\_VERSION _newversion_
|delete _filename_ \r\n| OK\r\n | ERR_FILE_NOT_FOUND


### Other errors
- ERR_REDIRECT <url> : Suggests that the node connected to is not the leader and gives the URL of the leader. If it does not know the leader an `_` is returned in place of the url field
- ERR_INTERNAL : Suggests an Internal error has occured and you need to retry later


### Some Conventions
- If the exptime given is 0, the file does not expire
- An update to expiry time resets the time left to expire
- 8095, 8096... 8099 are the ports of 5 nodes running

### Version
1.0.0

### Installation
    $ go get github.com/akhilsurya/akhilsurya
    $ go test -v github.com/akhilsurya/akhilsurya/cs733/assignment4 -run "^TestRPC_BasicSequential|TestRPC_Binary|TestRPC_Chunks|TestRPC_Batch|TestRPC_BasicTimer|TestRPC_ConcurrentWrites|TestRPC_ConcurrentCas|TestRPC_KillLeader$"
    $ go test -v github.com/akhilsurya/akhilsurya/cs733/assignment4 -run "^TestRaft1|TestRaft3$"
    $ go test -v github.com/akhilsurya/akhilsurya/cs733/assignment4 -run "^TestRaft1|TestRaft3$"




### TODO

 - Write Tests(There are never enough tests)
 - Add Code Comments
 - Make startup of the servers better i.e. Make each node start individually
 - Right now, one can only shutdown all the nodes together through the CLI




