// Package migrations embeds immutable control-plane schema migrations.
package migrations

import "embed"

//go:embed *.sql
var Files embed.FS
