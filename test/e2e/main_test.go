package e2e_test

import (
	"e2e/harness"
	"fmt"
	"log"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	log.Println("building collector image...")
	if err := harness.BuildCollectorImage(); err != nil {
		fmt.Fprintln(os.Stderr, "failed to build collector image:", err)
		os.Exit(1)
	}

	os.Exit(m.Run())
}
