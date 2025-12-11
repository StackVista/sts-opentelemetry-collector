package internal

import (
	"strings"

	"github.com/google/cel-go/common/ast"
)

const (
	indexFuncName = "_[_]"
	inFuncName    = "@in"
)

type Reference struct {
	Root string   // e.g., "resource"
	Path []string // e.g., ["attributes", "service.name"]
}

type ExpressionAstWalker struct {
	References map[string]Reference
	Literals   []interface{}
}

func NewExpressionAstWalker() *ExpressionAstWalker {
	return &ExpressionAstWalker{
		References: make(map[string]Reference),
		Literals:   make([]interface{}, 0),
	}
}

// Key Helper to create a unique Key for a reference
func (r Reference) Key() string {
	return r.Root + "." + strings.Join(r.Path, ".")
}

// Update addReference to use the map
func (w *ExpressionAstWalker) addReference(ref Reference) {
	w.References[ref.Key()] = ref
}

// GetReferences returns deduplicated references as a slice
func (w *ExpressionAstWalker) GetReferences() []Reference {
	refs := make([]Reference, 0, len(w.References))
	for _, ref := range w.References {
		refs = append(refs, ref)
	}
	return refs
}

func (w *ExpressionAstWalker) Walk(expr ast.Expr) {
	w.walk(expr, false)
}

// walk recursively traverses the CEL AST and collects attribute references.
func (w *ExpressionAstWalker) walk(expr ast.Expr, insideIndex bool) {
	if expr == nil {
		return
	}

	switch expr.Kind() {
	case ast.IdentKind:
		// Nothing to do for a bare identifier.

	case ast.SelectKind:
		if !insideIndex {
			// Record simple select references like vars.namespace.
			// Do not record bare map objects like *.attributes (handled by index '@in' cases).
			if root, path := extractRootAndPath(expr); root != "" && len(path) > 0 {
				w.addReference(Reference{Root: root, Path: path})
			}
		}
		sel := expr.AsSelect()
		w.walk(sel.Operand(), insideIndex)

	case ast.CallKind:
		call := expr.AsCall()

		// Handle nested index or 'in' operations within the path
		if call.FunctionName() == indexFuncName || call.FunctionName() == inFuncName {
			w.walkBinaryOpCall(call)
			return
		}

		if call.Target() != nil {
			w.walk(call.Target(), insideIndex)
		}
		for _, arg := range call.Args() {
			w.walk(arg, insideIndex)
		}

	case ast.ListKind:
		list := expr.AsList()
		for _, elem := range list.Elements() {
			w.walk(elem, insideIndex)
		}

	case ast.MapKind:
		mapExpr := expr.AsMap()
		for _, entry := range mapExpr.Entries() {
			w.walkEntry(entry, insideIndex)
		}

	case ast.StructKind:
		structExpr := expr.AsStruct()
		for _, field := range structExpr.Fields() {
			w.walkEntry(field, insideIndex)
		}

	case ast.ComprehensionKind:
		comp := expr.AsComprehension()
		w.walk(comp.IterRange(), insideIndex)
		w.walk(comp.AccuInit(), insideIndex)
		w.walk(comp.LoopCondition(), insideIndex)
		w.walk(comp.LoopStep(), insideIndex)
		w.walk(comp.Result(), insideIndex)

	case ast.LiteralKind:
		lit := expr.AsLiteral()
		w.Literals = append(w.Literals, lit.Value())

	case ast.UnspecifiedExprKind:
		// Ignore
	}
}

func (w *ExpressionAstWalker) walkEntry(entry ast.EntryExpr, insideIndex bool) {
	switch entry.Kind() {
	case ast.MapEntryKind:
		mapEntry := entry.AsMapEntry()
		w.walk(mapEntry.Key(), insideIndex)
		w.walk(mapEntry.Value(), insideIndex)

	case ast.StructFieldKind:
		structField := entry.AsStructField()
		// Only traverse the field value; the field name is not part of attribute paths we collect here.
		w.walk(structField.Value(), insideIndex)
	case ast.UnspecifiedEntryExprKind:
		// Ignore
	}
}

// extractRootAndPath walks backwards through a select / index chain to determine the
// root identifier (e.g., "resource", "span", "datapoint") and the field path from that root.
// For example:
//
//	datapoint.attributes               -> ("datapoint", ["attributes"])
//	resource.attributes["service.name"] -> ("resource", ["attributes", "service.name"])
//	a.b.c                              -> ("a", ["b", "c"])
func extractRootAndPath(expr ast.Expr) (string, []string) {
	path := make([]string, 0, 4)

	for expr != nil {
		switch expr.Kind() {
		case ast.SelectKind:
			sel := expr.AsSelect()
			// Prepend the field name to preserve order from root to leaf
			// We'll build by appending and reverse once at the end to avoid O(n^2) copies
			path = append(path, sel.FieldName())
			expr = sel.Operand()

		case ast.IdentKind:
			// Reverse the path to get root->leaf order because we collected from leaf to root
			for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
				path[i], path[j] = path[j], path[i]
			}
			return expr.AsIdent(), path

		case ast.CallKind:
			call := expr.AsCall()

			// Handle nested index or 'in' operations within the path
			if call.FunctionName() == indexFuncName || call.FunctionName() == inFuncName {
				return extractPathFromBinaryOp(call)
			}

			// Fallback: attempt to continue on target if present
			if call.Target() != nil {
				expr = call.Target()
				continue
			}
			return "", nil

		default:
			return "", nil
		}
	}
	return "", nil
}

func (w *ExpressionAstWalker) walkBinaryOpCall(call ast.CallExpr) {
	target, keyExpr := getTargetAndKeyExprForBinaryOp(call)

	if target == nil || keyExpr == nil {
		return
	}

	// Traverse children for completeness
	w.walk(target, true)
	w.walk(keyExpr, false)

	// Compute the concrete root and path from the target select chain
	tRoot, tPath := extractRootAndPath(target)
	if keyExpr.Kind() == ast.LiteralKind && tRoot != "" {
		if lit, ok := keyExpr.AsLiteral().Value().(string); ok {
			w.addReference(Reference{Root: tRoot, Path: append(tPath, lit)})
		}
	}
}

func extractPathFromBinaryOp(call ast.CallExpr) (string, []string) {
	target, keyExpr := getTargetAndKeyExprForBinaryOp(call)

	if target == nil || keyExpr == nil {
		return "", nil
	}

	// Compute the concrete root and path from the target select chain
	tRoot, tPath := extractRootAndPath(target)
	if keyExpr.Kind() == ast.LiteralKind {
		if s, ok := keyExpr.AsLiteral().Value().(string); ok {
			tPath = append(tPath, s)
		}
	}

	return tRoot, tPath
}

func getTargetAndKeyExprForBinaryOp(call ast.CallExpr) (ast.Expr, ast.Expr) {
	var target, keyExpr ast.Expr

	switch call.FunctionName() {
	case indexFuncName:
		// target[Key]
		if len(call.Args()) == 2 {
			target = call.Args()[0]
			keyExpr = call.Args()[1]
		}
	case inFuncName:
		// Key in target
		if len(call.Args()) == 2 {
			keyExpr = call.Args()[0]
			target = call.Args()[1]
		}
	default:
		return nil, nil
	}

	return target, keyExpr
}
