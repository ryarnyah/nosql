//go:build noplugin
// +build noplugin

package plugin

import "github.com/smallstep/nosql/database"

type DB = database.NotSupportedDB
