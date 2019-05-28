package main

import (
	"fmt"
	"flag"
	"os"
)

const buildVersion string = "v1.0.0"

var (
	showVer = flag.Bool("v", false, "show build version")
	buildName    string
	buildTime    string
	goVersion    string
	commitID     string
)

func init() {
	flag.Parse()

	if *showVer {
		fmt.Println("Build Name: ", buildName)
		fmt.Println("Build Version: ", buildVersion)
		fmt.Println("Build Time: ", buildTime)
		fmt.Println("Go Version: ", goVersion)
		fmt.Println("Commit ID: ", commitID)
		os.Exit(0)
	}
}

