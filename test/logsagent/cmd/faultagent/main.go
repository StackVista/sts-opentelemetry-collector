package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/format"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"go.yaml.in/yaml/v3"
)

type builderConfig struct {
	Dist struct {
		OutputPath string         `yaml:"output_path"`
		BuildTags  string         `yaml:"build_tags"`
		Other      map[string]any `yaml:",inline"`
	} `yaml:"dist"`
	Replaces []string             `yaml:"replaces"`
	Other    map[string]yaml.Node `yaml:",inline"`
}

type patch struct {
	Module string `json:"module"`
	File   string `json:"file"`
	Old    string `json:"old"`
	New    string `json:"new"`
	Source string `json:"source"`
}

func main() {
	output := flag.String("output", "", "output agent binary (required)")
	buildDir := flag.String("build-dir", "", "existing OCB-generated directory")
	flag.Parse()
	if err := build(*output, *buildDir); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func build(output, buildDir string) error {
	if output == "" || flag.NArg() != 0 {
		return errors.New("usage: faultagent -output binary [-build-dir directory]")
	}
	output, err := filepath.Abs(output)
	if err != nil {
		return err
	}
	root, err := repoRoot()
	if err != nil {
		return err
	}
	data, err := os.ReadFile(filepath.Join(root, "agent-otel-builder.yaml"))
	if err != nil {
		return err
	}
	var config builderConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("decode agent BOM: %w", err)
	}
	temp, err := os.MkdirTemp("", "logsagent-fault-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temp)

	if buildDir == "" {
		buildDir = filepath.Join(temp, "build")
		if err := generate(root, temp, buildDir, config); err != nil {
			return err
		}
	} else {
		buildDir, err = filepath.Abs(buildDir)
		if err != nil {
			return err
		}
	}
	modfile, err := copyModfile(buildDir, temp)
	if err != nil {
		return err
	}
	overlay, err := makeOverlay(root, buildDir, temp, modfile)
	if err != nil {
		return err
	}
	return goCommand(buildDir, "build", "-race", "-p", "2", "-buildvcs=false", "-modfile="+modfile, "-overlay="+overlay,
		"-tags="+config.Dist.BuildTags, "-o="+output, ".").Run()
}

func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "agent-otel-builder.yaml")); err == nil {
			return dir, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("agent-otel-builder.yaml not found above working directory")
		}
		dir = parent
	}
}

func generate(root, temp, buildDir string, config builderConfig) error {
	for key, node := range config.Other {
		absolutePaths(&node, root)
		config.Other[key] = node
	}
	for i, replacement := range config.Replaces {
		module, target, ok := strings.Cut(replacement, "=>")
		if !ok {
			return fmt.Errorf("invalid replacement: %q", replacement)
		}
		target = strings.TrimSpace(target)
		if target == "." || target == ".." || strings.HasPrefix(target, "./") || strings.HasPrefix(target, "../") {
			// BOM replacements are relative to the repository's generated bin directory.
			config.Replaces[i] = strings.TrimSpace(module) + " => " + filepath.Join(root, "bin", target)
		}
	}
	config.Dist.OutputPath = buildDir
	data, err := yaml.Marshal(config)
	if err != nil {
		return err
	}
	configPath := filepath.Join(temp, "builder.yaml")
	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		return err
	}
	return goCommand(root, "run", "go.opentelemetry.io/collector/cmd/builder@v0.153.0",
		"--config="+configPath, "--skip-compilation").Run()
}

func absolutePaths(node *yaml.Node, root string) {
	if node.Kind == yaml.MappingNode {
		for i := 0; i < len(node.Content); i += 2 {
			key, value := node.Content[i], node.Content[i+1]
			if key.Value == "path" && value.Kind == yaml.ScalarNode && value.Value != "" &&
				!filepath.IsAbs(value.Value) {
				value.Value = filepath.Join(root, value.Value)
			}
		}
	}
	for _, child := range node.Content {
		absolutePaths(child, root)
	}
}

func copyModfile(buildDir, temp string) (string, error) {
	for _, name := range []string{"go.mod", "go.sum"} {
		data, err := os.ReadFile(filepath.Join(buildDir, name))
		if name == "go.sum" && errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return "", err
		}
		//nolint:gosec // The filename is fixed to go.mod or go.sum in our temporary directory.
		if err := os.WriteFile(filepath.Join(temp, name), data, 0o600); err != nil {
			return "", err
		}
	}
	modfile := filepath.Join(temp, "go.mod")
	var stdout bytes.Buffer
	cmd := goCommand(buildDir, "mod", "edit", "-modfile="+modfile, "-json")
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		return "", err
	}
	var parsed struct {
		Replace []struct {
			Old, New struct{ Path, Version string }
		}
	}
	if err := json.Unmarshal(stdout.Bytes(), &parsed); err != nil {
		return "", err
	}
	args := []string{"mod", "edit", "-modfile=" + modfile}
	for _, replacement := range parsed.Replace {
		if replacement.New.Version != "" || filepath.IsAbs(replacement.New.Path) {
			continue
		}
		old := replacement.Old.Path
		if replacement.Old.Version != "" {
			old += "@" + replacement.Old.Version
		}
		target := filepath.Join(buildDir, replacement.New.Path)
		args = append(args, "-replace="+old+"="+target)
	}
	if len(args) > 3 {
		if err := goCommand(buildDir, args...).Run(); err != nil {
			return "", err
		}
	}
	return modfile, nil
}

func makeOverlay(root, buildDir, temp, modfile string) (string, error) {
	faults := filepath.Join(root, "test", "logsagent", "testdata", "faults")
	data, err := os.ReadFile(filepath.Join(faults, "patches.json"))
	if err != nil {
		return "", err
	}
	var patches []patch
	if err := json.Unmarshal(data, &patches); err != nil {
		return "", fmt.Errorf("decode fault patches: %w", err)
	}
	modules := map[string]string{"": root}
	files := make(map[string][]byte)
	for i, p := range patches {
		if !filepath.IsLocal(p.File) {
			return "", fmt.Errorf("patch %d: file must be relative to its module", i+1)
		}
		dir, ok := modules[p.Module]
		if !ok {
			var stdout bytes.Buffer
			cmd := goCommand(buildDir, "list", "-modfile="+modfile, "-m", "-f", "{{.Dir}}", p.Module)
			cmd.Stdout = &stdout
			if err := cmd.Run(); err != nil {
				return "", fmt.Errorf("resolve module %s: %w", p.Module, err)
			}
			dir = strings.TrimSpace(stdout.String())
			if !filepath.IsAbs(dir) {
				return "", fmt.Errorf("module %s returned no absolute directory", p.Module)
			}
			copied := filepath.Join(temp, fmt.Sprintf("module-%d", len(modules)))
			if err := os.CopyFS(copied, os.DirFS(dir)); err != nil {
				return "", fmt.Errorf("copy module %s: %w", p.Module, err)
			}
			if err := goCommand(buildDir, "mod", "edit", "-modfile="+modfile,
				"-replace="+p.Module+"="+copied).Run(); err != nil {
				return "", err
			}
			dir = copied
			modules[p.Module] = copied
		}
		target := filepath.Join(dir, p.File)
		content, err := applyPatch(files, target, faults, p)
		if err != nil {
			return "", fmt.Errorf("patch %d (%s): %w", i+1, target, err)
		}
		files[target] = content
	}
	replacements := make(map[string]string, len(files))
	for target, content := range files {
		if filepath.Ext(target) == ".go" {
			content, err = format.Source(content)
			if err != nil {
				return "", fmt.Errorf("format %s: %w", target, err)
			}
		}
		backing := filepath.Join(temp, fmt.Sprintf("patch-%d", len(replacements)))
		if err := os.WriteFile(backing, content, 0o600); err != nil {
			return "", err
		}
		replacements[target] = backing
	}
	data, err = json.Marshal(struct {
		Replace map[string]string
	}{Replace: replacements})
	if err != nil {
		return "", err
	}
	overlay := filepath.Join(temp, "overlay.json")
	if err := os.WriteFile(overlay, data, 0o600); err != nil {
		return "", err
	}
	return overlay, nil
}

func applyPatch(files map[string][]byte, target, faults string, p patch) ([]byte, error) {
	content, patched := files[target]
	if p.Old == "" {
		if p.New != "" || !filepath.IsLocal(p.Source) {
			return nil, errors.New("additions require a relative source and no new text")
		}
		if patched {
			return nil, errors.New("addition target already patched")
		}
		if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
			if err != nil {
				return nil, err
			}
			return nil, errors.New("addition target already exists")
		}
		return os.ReadFile(filepath.Join(faults, p.Source))
	}
	if p.Source != "" {
		return nil, errors.New("source is only valid for additions")
	}
	if !patched {
		var err error
		content, err = os.ReadFile(target)
		if err != nil {
			return nil, err
		}
	}
	if count := bytes.Count(content, []byte(p.Old)); count != 1 {
		return nil, fmt.Errorf("old text matched %d times; want exactly one", count)
	}
	return bytes.Replace(content, []byte(p.Old), []byte(p.New), 1), nil
}

func goCommand(dir string, args ...string) *exec.Cmd {
	cmd := exec.Command("go", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOWORK=off")
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	fmt.Fprintf(os.Stderr, "(%s) %s\n", dir, cmd.String())
	return cmd
}
