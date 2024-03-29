<p align="center">
  <img src="https://user-images.githubusercontent.com/29172543/110962237-f1bee380-837a-11eb-91ad-1319657c7dc0.png" width="75" alt="accessibility text">
</p>

# Hariken

## Install
Use the following guide to install Go - https://golang.org/doc/install

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
go test -v -coverpkg=./... -coverprofile=profile.cov ./...

# Coverage in CLI
go tool cover -func profile.cov

# Coverage in Browser
go tool cover -html=profile.cov
```
