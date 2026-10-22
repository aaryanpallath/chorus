// Package web embeds the demo chat client served by the edge.
package web

import "embed"

//go:embed index.html
var Files embed.FS
