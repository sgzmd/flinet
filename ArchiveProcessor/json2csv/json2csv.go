package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

var input string
var output string
var discardFirstWords int

func main() {
	flag.StringVar(&input, "input", "", "Input file path")
	flag.StringVar(&output, "output", "", "Output file path")
	flag.IntVar(&discardFirstWords, "discard_first_words", 0,
		"Number of first words to discard from body")
	flag.Parse()
	flag.Parse()

	_, err := os.Stat(output)
	if err == nil {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Output file already exists. Do you want to overwrite it? (y/n): ")
		response, _ := reader.ReadString('\n')

		if response != "y\n" {
			fmt.Println("Aborted.")
			os.Exit(1)
		}
	}

	// Continue with your code here
	file, err := os.Open(input)
	if err != nil {
		panic(err)
	}

	scanner := bufio.NewScanner(file)
	csvFile, err := os.Create(output)
	if err != nil {
		panic(err)
	}
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)
	defer writer.Flush()
	err = writer.Write([]string{"body", "genre"})
	if err != nil {
		panic(err)
	}

	for scanner.Scan() {
		line := scanner.Text()
		var jsonObj map[string]interface{}
		err := json.Unmarshal([]byte(line), &jsonObj)
		if err != nil {
			if len(line) > 100 {
				line = line[:100]
			}
			log.Printf("Error parsing line: %s because %+v", line, err)
			continue
		}

		body, ok := jsonObj["body"].(string)
		if !ok {
			log.Println("Error: body is not a string")
			continue
		}

		words := strings.Fields(body)
		if len(words) > discardFirstWords {
			words = words[discardFirstWords:]
		}
		body = strings.Join(words, " ")

		genreInterface, ok := jsonObj["genre"].([]interface{})
		if !ok {
			log.Println("Error: genre is not a list of strings")
			continue
		}

		genre := make([]string, len(genreInterface))
		for i, v := range genreInterface {
			genre[i], ok = v.(string)
			if !ok {
				log.Println("Error: genre element is not a string")
				continue
			}
		}

		err = writer.Write([]string{body, strings.Join(genre, ",")})
		if err != nil {
			panic(err)
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}
}
