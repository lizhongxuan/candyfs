package main

import (
	"candyfs/cmd"
	"candyfs/utils/log"
	"os"
)

func main() {
	err := cmd.Main(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
