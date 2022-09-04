package protocol

import "errors"

var (
	ErrApiKey            = errors.New("unexpected api key")
	ErrTransportShutdown = errors.New("transport shutdown")
	ErrCommand           = errors.New("unexpected command")
)
