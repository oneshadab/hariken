# Hariken

## Install

```
git clone git@github.com:oneshadab/hariken.git
cd hariken
go install .
```

###  Run
```
hariken [command]
```

Valid values for `command`:

- `startServerAndConnect` (default) - creates a server and connects to it
-  `connect` - connects to a running server using the default connection string
-  `startServer` - creates a server that can be connected to with connection string

### Tests
```
go test -v ./... -cover
```