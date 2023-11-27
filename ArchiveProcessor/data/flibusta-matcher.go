package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/schollz/progressbar/v3"
)

func main() {
	// Command-line flags
	host := flag.String("host", "localhost", "Database host")
	port := flag.String("port", "3306", "Database port")
	username := flag.String("username", "root", "Database username")
	dbName := flag.String("dbname", "", "Database name")
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 || *dbName == "" {
		fmt.Println("Usage: go run script.go [options] [CSV file path] [Output CSV file path]")
		fmt.Println("Options:")
		flag.PrintDefaults()
		return
	}

	csvFilePath := args[0]
	outputCSVPath := args[1]

	// Securely get password
	fmt.Print("Enter your MySQL password: ")
	reader := bufio.NewReader(os.Stdin)
	password, _ := reader.ReadString('\n')

	// Connect to the database
	connectionString := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s",
		*username,
		strings.TrimSpace(password), // comes with \n on the end
		*host,
		*port,
		*dbName)

	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Open the input CSV file
	file, err := os.Open(csvFilePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	csvReader := csv.NewReader(file)
	records, err := csvReader.ReadAll()
	if err != nil {
		panic(err)
	}

	// Create and open the output CSV file
	outputFile, err := os.Create(outputCSVPath)
	if err != nil {
		panic(err)
	}
	defer outputFile.Close()

	csvWriter := csv.NewWriter(outputFile)
	defer csvWriter.Flush()

	progressBar := progressbar.Default(int64(len(records)))

	csvWriter.Write([]string{
		"book_id",
		"original_title",
		"original_author",
		"matched_book_title",
		"matched_seq_name",
		"matched_first_name",
		"matched_last_name",
		"relevance_title",
		"relevance_author"})

	for _, record := range records {
		title := record[0]
		author := record[1]

		query := `SELECT BookId, Title, SeqName, FirstName, LastName,
                  MATCH(Title, SeqName) AGAINST(? IN NATURAL LANGUAGE MODE) AS relevance_title, 
                  MATCH(FirstName, LastName, MiddleName, NickName) AGAINST(? IN NATURAL LANGUAGE MODE) AS relevance_author 
                  FROM BooksFT 
                  WHERE MATCH(Title, SeqName) AGAINST(? IN NATURAL LANGUAGE MODE) 
                  AND MATCH(FirstName, LastName, MiddleName, NickName) AGAINST(? IN NATURAL LANGUAGE MODE)
									LIMIT 1;`

		rows, err := db.Query(query, title, author, title, author)
		if err != nil {
			panic(err)
		}

		for rows.Next() {
			var bookId int
			var bookTitle, seqName, firstName, lastName string
			var relevanceTitle, relevanceAuthor float64
			err := rows.Scan(&bookId, &bookTitle, &seqName, &firstName, &lastName, &relevanceTitle, &relevanceAuthor)
			if err != nil {
				panic(err)
			}

			if relevanceTitle < 10 || relevanceAuthor < 8 {
				continue
			}

			// Write the result to the output CSV file
			err = csvWriter.Write([]string{
				fmt.Sprintf("%d", bookId),
				title,
				author,
				bookTitle,
				seqName,
				firstName,
				lastName,
				fmt.Sprintf("%f", relevanceTitle),
				fmt.Sprintf("%f", relevanceAuthor)})
			if err != nil {
				panic(err)
			}
		}

		rows.Close()
		progressBar.Add(1)
	}
}
