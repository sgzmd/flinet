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
var fieldsToExtract string

type Task struct {
	Body      string
	Positives map[string]bool
	Negatives map[string]bool
}

type Result struct {
	WorkedID        int
	Book            Book
	MatchedPositive string
}

// Author represents the author of the book.
type Author struct {
	FirstName  string `json:"first_name"`
	LastName   string `json:"last_name"`
	MiddleName string `json:"middle_name"`
	NickName   string `json:"nick_name"`
}

// Book represents the book details.
type Book struct {
	ID         string   `json:"id"`
	Genre      []string `json:"genre"`
	Author     []Author `json:"author"`
	BookTitle  string   `json:"book_title"`
	Body       string   `json:"body"`
	Annotation string   `json:"annotation"`
	FileName   string   `json:"file_name"`
	IsSelected string   `json:"is_selected"`
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

// CSVRecord converts the Book struct into a slice of strings suitable for CSV
// output.
// TODO: keep in sync with CSVHeader
func (b *Book) CSVRecord() []string {
	authors := make([]string, 0, len(b.Author))
	for _, author := range b.Author {
		authors = append(authors, fmt.Sprintf("%s %s", author.FirstName, author.LastName))
	}

	return []string{
		b.ID,
		strings.Join(b.Genre, ";"),
		strings.Join(authors, ";"),
		b.BookTitle,
		b.Body,
		b.Annotation,
		b.FileName,
		b.IsSelected,
	}
}

// CSVHeader returns the header for the CSV file.
// TODO: keep in sync with Book.CSVRecord
func CSVHeader() []string {
	return []string{
		"ID",
		"Genres",
		"Authors",
		"BookTitle",
		"Body",
		"Annotation",
		"FileName",
		"IsSelected",
	}
}

func processLine(id int, tasks <-chan Task, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()
	for task := range tasks {
		var book Book
		err := json.Unmarshal([]byte(task.Body), &book)
		if err != nil {
			log.Printf("Error parsing line: %s because %+v", task.Body, err)
			continue
		}

		book.IsSelected = "0"
		if task.Positives[book.FileName] {
			log.Printf("Positive: %s", book.FileName)
			book.IsSelected = "1"
		} else if task.Negatives[book.FileName] {
			log.Printf("Negative: %s", book.FileName)
			book.IsSelected = "-1"
		}

		result := Result{
			WorkedID: id,
			Book:     book,
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
	err = writer.Write(CSVHeader())
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
		err := writer.Write(result.Book.CSVRecord())
		if err != nil {
			panic(err)
		}
		if result.Book.IsSelected == "1" {
			matchedPositives = append(matchedPositives, result.Book.FileName)
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
