// Package kafkaext exposes embedded assets owned by the kafka-phoenix-ext
// extension.
package kafkaext

import "embed"

// Migrations holds this repo's SQL, applied by the extension on start.
// Phoenix never owns these tables.
//
//go:embed migrations/*.sql
var Migrations embed.FS
