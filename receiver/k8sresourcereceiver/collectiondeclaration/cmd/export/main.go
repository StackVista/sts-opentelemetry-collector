package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/collectiondeclaration"
)

func main() {
	output := flag.String("output", "", "destination path; omit to write stdout")
	flag.Parse()
	if *output == "" {
		if _, err := os.Stdout.Write(collectiondeclaration.CRD); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if err := os.WriteFile(*output, collectiondeclaration.CRD, 0o600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
