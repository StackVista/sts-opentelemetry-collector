package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/stackvista/sts-opentelemetry-collector/receiver/k8sresourcereceiver/resourcecollection"
)

func main() {
	output := flag.String("output", "", "destination path; omit to write stdout")
	flag.Parse()
	if *output == "" {
		if _, err := os.Stdout.Write(resourcecollection.CRD); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if err := os.WriteFile(*output, resourcecollection.CRD, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
