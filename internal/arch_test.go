package internal

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const modulePrefix = "github.com/rasonyang/cascade-realtime-gateway/internal/"

// forbidden lists, per internal package (path relative to internal/, prefix
// match), the internal packages it must never import. "*" means every
// internal package: the package is a leaf.
var forbidden = map[string][]string{
	"config":        {"*"},
	"observability": {"*"},
	"audio":         {"*"},
	"vad":           {"config", "observability", "provider", "session", "protocol", "server", "recorder"}, // may use audio only
	"provider":      {"session", "protocol", "server"},
	"session":       {"protocol", "server"},
	"protocol":      {"server"},
	"recorder":      {"protocol", "server"},
}

// TestImportDirection scans every non-test Go file under internal/ and fails
// on any import that violates the layering table above.
func TestImportDirection(t *testing.T) {
	root := "."
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		pkgDir := filepath.ToSlash(filepath.Dir(path))
		if pkgDir == "." {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imp := range f.Imports {
			target, _ := strconv.Unquote(imp.Path.Value)
			if !strings.HasPrefix(target, modulePrefix) {
				continue
			}
			rel := strings.TrimPrefix(target, modulePrefix)
			if violates(pkgDir, rel) {
				t.Errorf("%s: package %q must not import %q", path, pkgDir, rel)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat("doc.go"); err != nil {
		t.Fatal("arch_test must run from the internal/ directory")
	}
}

func violates(pkgDir, importRel string) bool {
	top := strings.SplitN(pkgDir, "/", 2)[0]
	rules, ok := forbidden[top]
	if !ok {
		return false
	}
	importTop := strings.SplitN(importRel, "/", 2)[0]
	if importTop == top {
		return false // subpackages of the same top-level package are fine
	}
	for _, r := range rules {
		if r == "*" || r == importTop {
			return true
		}
	}
	return false
}
