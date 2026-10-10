// Copyright (c) 2026 Go-Helix Authors
//
// This file is part of go-helix.
// Licensed under the MIT License. See the LICENSE file in the project root
// for the full license text.

package web

import (
	"embed"
	"io/fs"
)

// staticFS 存放内嵌的管理控制台前端。
//
//go:embed all:static
var staticFS embed.FS

// StaticFS 返回以 "static" 为根目录的内嵌前端文件系统。
func StaticFS() (fs.FS, error) {
	return fs.Sub(staticFS, "static")
}
