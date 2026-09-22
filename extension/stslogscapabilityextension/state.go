package stslogscapabilityextension

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/stackvista/sts-opentelemetry-collector/common/logsagent"
)

type restartState struct {
	SchemaVersion int            `json:"schema_version"`
	LastAttemptAt time.Time      `json:"last_attempt_at"`
	OldMode       logsagent.Mode `json:"old_mode"`
	NewMode       logsagent.Mode `json:"new_mode"`
	PendingIntent bool           `json:"pending_intent"`
}

func loadState(directory string) (restartState, error) {
	var state restartState
	file, err := os.Open(filepath.Join(directory, "state.json"))
	if errors.Is(err, os.ErrNotExist) {
		return restartState{SchemaVersion: 1}, nil
	}
	if err != nil {
		return state, errors.New("cannot read existing logs controller state")
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 16385))
	if err != nil || len(data) > 16384 {
		return state, errors.New("cannot read existing logs controller state")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil {
		return state, errors.New("invalid logs controller state")
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return state, errors.New("invalid trailing logs controller state")
	}
	validMode := func(mode logsagent.Mode) bool {
		return mode == logsagent.PromtailMode || mode == logsagent.OTELNativeMode
	}
	if state.SchemaVersion != 1 || state.LastAttemptAt.IsZero() ||
		!validMode(state.OldMode) || !validMode(state.NewMode) || state.OldMode == state.NewMode {
		return state, errors.New("invalid logs controller state fields")
	}
	return state, nil
}

func saveState(directory string, state restartState) error {
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return errors.New("cannot create logs controller state directory")
	}
	file, err := os.CreateTemp(directory, ".state-*")
	if err != nil {
		return errors.New("cannot create logs controller state")
	}
	defer os.Remove(file.Name())
	if err := json.NewEncoder(file).Encode(state); err != nil {
		file.Close()
		return errors.New("cannot encode logs controller state")
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return errors.New("cannot sync logs controller state")
	}
	if err := file.Close(); err != nil {
		return errors.New("cannot close logs controller state")
	}
	if err := os.Rename(file.Name(), filepath.Join(directory, "state.json")); err != nil {
		return errors.New("cannot replace logs controller state")
	}
	return nil
}
