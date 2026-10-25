package m12static

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// Deprecated symbols from docs/7.2 plan.md task 12.6. They must not appear in
// controller/ or agent/ source. Comments and absence-assertion tests may still
// mention them by name; the scanners below therefore inspect parsed Go
// identifiers (and comment-stripped proto text) rather than raw lines.
// The names are assembled from fragments so this checker does not itself add
// literal hits to the acceptance grep it implements.
const (
	symA = "Get" + "Preheat" + "Progress"
	symB = "ScorePolicy" + "." + "Window"
	symC = "consume" + "Order" + "Ref"
)

// TestM12NoDeprecatedSymbols is the repeatable static check for task 12.6.
func TestM12NoDeprecatedSymbols(t *testing.T) {
	root := m12ModuleRoot(t)
	var hits []string

	for _, dir := range []string{
		filepath.Join(root, "controller"),
		filepath.Join(root, "agent"),
	} {
		err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				switch entry.Name() {
				case "vendor", "testdata", ".git":
					return filepath.SkipDir
				}
				return nil
			}
			switch filepath.Ext(path) {
			case ".go":
				if sameFile(path, t) {
					return nil
				}
				src, readErr := os.ReadFile(path)
				if readErr != nil {
					return readErr
				}
				found, scanErr := m12ScanGo(path, src)
				if scanErr != nil {
					return scanErr
				}
				hits = append(hits, found...)
			case ".proto":
				src, readErr := os.ReadFile(path)
				if readErr != nil {
					return readErr
				}
				hits = append(hits, m12ScanProto(path, src)...)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("scan %s: %v", dir, err)
		}
	}

	if len(hits) > 0 {
		t.Fatalf("deprecated symbols still present:\n%s", strings.Join(hits, "\n"))
	}
}

// TestM12StaticScannerIsNotVacuous proves the scanners find each symbol in
// parsed source and ignore the same text in comments. Without this, a typo in
// the scanner could make TestM12NoDeprecatedSymbols pass by accident.
func TestM12StaticScannerIsNotVacuous(t *testing.T) {
	src := []byte(fmt.Sprintf(`package sample

// %s %s %s stay in comments.

var _ = %s
var _ = %s
var _ = ScorePolicy{Window: 1}
var %s = 1
`, symA, symC, symB,
		symA, symB, symC))
	hits, err := m12ScanGo("sample.go", src)
	if err != nil {
		t.Fatalf("scan synthetic Go: %v", err)
	}
	for _, want := range []string{
		symA, symB, symC,
	} {
		if !m12ContainsHit(hits, want) {
			t.Errorf("scanner missed %s in synthetic source: %v", want, hits)
		}
	}

	commentOnly, err := m12ScanGo("comments.go", []byte(fmt.Sprintf(`package sample
// %s %s %s
`, symA, symC, symB)))
	if err != nil {
		t.Fatalf("scan comment-only Go: %v", err)
	}
	if len(commentOnly) != 0 {
		t.Errorf("comments produced hits: %v", commentOnly)
	}

	testFile, err := m12ScanGo("sample_test.go", []byte(fmt.Sprintf(`package sample
var %s = 1
var _ = %s
`, symC, symA)))
	if err != nil {
		t.Fatalf("scan synthetic test file: %v", err)
	}
	if m12ContainsHit(testFile, symC) {
		t.Errorf("test-only identifier %s should not be a production hit: %v", symC, testFile)
	}
	if !m12ContainsHit(testFile, symA) {
		t.Errorf("scanner missed %s in test file: %v", symA, testFile)
	}

	protoHits := m12ScanProto("sample.proto", []byte(fmt.Sprintf(`
// %s is deprecated
message Sample {
  string %s = 1;
  string keep = 2; // %s
}
`, symC, symC, symA)))
	if len(protoHits) != 1 || !m12ContainsHit(protoHits, symC) {
		t.Errorf("proto scan = %v, want exactly one %s hit", protoHits, symC)
	}
}

type m12GoScanner struct {
	fset     *token.FileSet
	filename string
	testFile bool
	hits     []string
}

func (s *m12GoScanner) Visit(node ast.Node) ast.Visitor {
	switch n := node.(type) {
	case *ast.Ident:
		if n.Name == symA {
			s.add(n.Pos(), n.Name)
		}
		// A test file may name a local variable after the removed field while
		// asserting that the CRD no longer exposes it. Production files may not.
		if !s.testFile && n.Name == symC {
			s.add(n.Pos(), n.Name)
		}
	case *ast.SelectorExpr:
		if ident, ok := n.X.(*ast.Ident); ok &&
			ident.Name == "ScorePolicy" && n.Sel.Name == "Window" {
			s.add(n.Pos(), symB)
		}
	case *ast.CompositeLit:
		ident, ok := n.Type.(*ast.Ident)
		if !ok || ident.Name != "ScorePolicy" {
			break
		}
		for _, elt := range n.Elts {
			if kv, ok := elt.(*ast.KeyValueExpr); ok {
				if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "Window" {
					s.add(kv.Pos(), symB)
				}
			}
		}
	case *ast.TypeSpec:
		// A reintroduced `type ScorePolicy struct { Window ... }` must be
		// caught even though it is not a selector expression.
		structType, ok := n.Type.(*ast.StructType)
		if !ok || n.Name.Name != "ScorePolicy" {
			break
		}
		for _, field := range structType.Fields.List {
			for _, name := range field.Names {
				if name.Name == "Window" {
					s.add(name.Pos(), symB)
				}
			}
		}
	}
	return s
}

func (s *m12GoScanner) add(pos token.Pos, symbol string) {
	position := s.fset.Position(pos)
	s.hits = append(s.hits, fmt.Sprintf("%s:%d:%d: %s",
		s.filename, position.Line, position.Column, symbol))
}

func m12ScanGo(filename string, src []byte) ([]string, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filename, src, parser.SkipObjectResolution)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", filename, err)
	}
	scanner := &m12GoScanner{
		fset:     fset,
		filename: filename,
		testFile: strings.HasSuffix(filename, "_test.go"),
	}
	ast.Walk(scanner, file)
	return scanner.hits, nil
}

func m12ScanProto(filename string, src []byte) []string {
	body := m12StripProtoComments(string(src))
	var hits []string
	for _, symbol := range []string{symA, symB, symC} {
		if !strings.Contains(body, symbol) {
			continue
		}
		line := 1 + strings.Count(body[:strings.Index(body, symbol)], "\n")
		hits = append(hits, fmt.Sprintf("%s:%d: %s", filename, line, symbol))
	}
	return hits
}

// m12StripProtoComments removes // and /* */ comments while preserving line
// breaks so reported line numbers stay meaningful.
func m12StripProtoComments(src string) string {
	var out strings.Builder
	for i := 0; i < len(src); {
		switch {
		case strings.HasPrefix(src[i:], "//"):
			for i < len(src) && src[i] != '\n' {
				i++
			}
		case strings.HasPrefix(src[i:], "/*"):
			i += 2
			for i < len(src) && !strings.HasPrefix(src[i:], "*/") {
				if src[i] == '\n' {
					out.WriteByte('\n')
				}
				i++
			}
			if i < len(src) {
				i += 2
			}
		default:
			out.WriteByte(src[i])
			i++
		}
	}
	return out.String()
}

func m12ContainsHit(hits []string, symbol string) bool {
	for _, hit := range hits {
		if strings.HasSuffix(hit, ": "+symbol) {
			return true
		}
	}
	return false
}

func m12ModuleRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// .../controller/internal/<this file> -> repository root.
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func sameFile(path string, t *testing.T) bool {
	t.Helper()
	_, self, _, ok := runtime.Caller(0)
	if !ok {
		return false
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	absSelf, err := filepath.Abs(self)
	if err != nil {
		return false
	}
	return absPath == absSelf
}
