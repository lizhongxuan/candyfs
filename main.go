package main

import (
	"os"
	"candyfs/cmd"
	"candyfs/logger"
)

func main() {
	err := cmd.Main(os.Args)
	if err != nil {
		logger.Fatal(err)
	}
}
