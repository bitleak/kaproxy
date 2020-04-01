package server

// please update these using `go build -ldflags "-X server.Version=0.0.1 -X server.BuildDate=2017.04.24" -X server.BuildCommit=asdf`
var (
	Version     = "unknown"
	BuildDate   = "unknown"
	BuildCommit = "unknown"
)
