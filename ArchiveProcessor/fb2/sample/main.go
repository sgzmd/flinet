package main

import (
	"ArchiveProcessor/fb2"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const N_CHARS = 2000

func main() {
	// Get the path from the command line.
	path := os.Args[1]

	// List all the files in the directory.
	files, err := filepath.Glob(filepath.Join(path, "*.fb2"))
	if err != nil {
		panic(err)
	}

	// Do something with each file.
	for _, file := range files {
		// Do something with the file.
		data, err := os.ReadFile(file)
		if err != nil {
			panic(err)
		}

		book, err := fb2.ParseFictionBook(data)
		if err != nil {
			panic(err)
		}

		j := book.Flatten()
		js, _ := json.Marshal(j)

		fmt.Println(string(js))
	}
}
