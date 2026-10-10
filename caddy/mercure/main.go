// Package main builds the Mercure hub with the MongoDB transport.
//
// The binary is the standard Caddy command with the Mercure handler and the
// MongoDB transport plugged in, so the image is ready to run with a Caddyfile.
package main

import (
	caddycmd "github.com/caddyserver/caddy/v2/cmd"
	// Plug in Caddy modules here.
	_ "github.com/caddyserver/caddy/v2/modules/standard"
	_ "github.com/dunglas/mercure/caddy"

	_ "github.com/GromNaN/mercure-mongodb"
)

func main() {
	caddycmd.Main()
}
