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
	"sync"
)

const maxCapacity = 5 * 1024 * 1024 * 1024

var input string
var output string
var discardFirstWords int
var useFiles bool
var positiveSamples string
var negativesSamples string
var matchedPositivesOutput string

type Task struct {
	Body      string
	Positives map[string]bool
	Negatives map[string]bool
}

type Result struct {
	WorkedID        int
	Fields          []string
	MatchedPositive string
}

func getFiles(fileName string) []string {
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Printf("Error opening file %s: %v\n", fileName, err)
		os.Exit(1)
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading file %s: %v\n", fileName, err)
	}

	return lines
}

func processLine(id int, tasks <-chan Task, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()
	for task := range tasks {
		var jsonObj map[string]interface{}
		line := task.Body
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

		annotation, ok := jsonObj["annotation"].(string) // Get the "annotation" field
		if !ok {
			annotation = "" // Set a default value if "annotation" is not present
		}

		fileName, ok := jsonObj["file_name"].(string)
		if !ok {
			log.Println("Error: file_name is not a string")
			continue
		}

		isSelected := "0"
		if task.Positives[fileName] {
			log.Printf("Positive: %s", fileName)
			isSelected = "1"
		} else if task.Negatives[fileName] {
			log.Printf("Negative: %s", fileName)
			isSelected = "-1"
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

		bodyWithAnnotation := annotation + " " + body // Concatenate "annotation" with "body"

		fields := []string{
			bodyWithAnnotation,
			strings.Join(genre, ","),
			isSelected,
			fileName}

		result := Result{
			WorkedID: id,
			Fields:   fields,
		}

		if isSelected == "1" {
			result.MatchedPositive = fileName
		} else {
			result.MatchedPositive = ""
		}

		results <- result
	}
}

func main() {
	flag.StringVar(&input, "input", "", "Input file path")
	flag.StringVar(&output, "output", "", "Output file path")
	flag.IntVar(&discardFirstWords, "discard_first_words", 0,
		"Number of first words to discard from body")
	flag.StringVar(&positiveSamples, "positive_samples", "", "Positive samples file path")
	flag.StringVar(&negativesSamples, "negative_samples", "", "Negatives sample file path")
	flag.StringVar(&matchedPositivesOutput, "matched_positives_output", "", "Where to store matched positives")
	flag.BoolVar(&useFiles, "use_files", true, "Use files for custom labelling")
	flag.Parse()

	if len(positiveSamples) < 1 {
		log.Fatal("Positive samples file is required")
	}

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
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)
	csvFile, err := os.Create(output)
	if err != nil {
		panic(err)
	}
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)
	defer writer.Flush()
	err = writer.Write([]string{"body", "genre", "selected", "file_name"}) // Remove "annotation" from the header
	if err != nil {
		panic(err)
	}

	positives := getFiles(positiveSamples)
	negatives := getFiles(negativesSamples)

	positiveMap := make(map[string]bool)
	for _, v := range positives {
		positiveMap[v+".fb2"] = true
	}

	negativeMap := make(map[string]bool)
	for _, v := range negatives {
		negativeMap[v+".fb2"] = true
	}

	const numWorkers = 5
	var wg sync.WaitGroup

	tasks := make(chan Task, 500000)
	results := make(chan Result, 500000)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go processLine(i, tasks, results, &wg)
	}

	log.Printf("Reading input file...")
	for scanner.Scan() {
		line := scanner.Text()
		tasks <- Task{
			Body:      line,
			Positives: positiveMap,
			Negatives: negativeMap,
		}
	}
	close(tasks)

	log.Printf("Waiting for workers to finish...")
	go func() {
		wg.Wait()
		close(results)
	}()

	log.Printf("Writing results...")
	matchedPositives := make([]string, 0)
	// bar := pb.New(len(results))
	for result := range results {
		err := writer.Write(result.Fields)
		if err != nil {
			panic(err)
		}
		if result.MatchedPositive != "" {
			matchedPositives = append(matchedPositives, result.MatchedPositive)
		}
	}

	log.Printf("Matched positives: %d", len(matchedPositives))
	if matchedPositivesOutput != "" {
		matchedPositivesFile, err := os.Create(matchedPositivesOutput)
		if err != nil {
			panic(err)
		}
		defer matchedPositivesFile.Close()

		matchedPositivesWriter := bufio.NewWriter(matchedPositivesFile)
		defer matchedPositivesWriter.Flush()

		for _, v := range matchedPositives {
			matchedPositivesWriter.WriteString(v + "\n")
		}
	}

	// bar.Finish()

	if err := scanner.Err(); err != nil {
		panic(err)
	}
}
