package main

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/anaskhan96/soup"
	pb "github.com/schollz/progressbar/v3"
	"golang.org/x/net/html/charset"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
)

type Author struct {
	FirstName  string `json:"first_name"`
	LastName   string `json:"last_name"`
	MiddleName string `json:"middle_name"`
	NickName   string `json:"nick_name"`
}

type Data struct {
	ID         string   `json:"id"`
	Genres     []string `json:"genre"`
	Authors    []Author `json:"author"`
	BookTitle  string   `json:"book_title"`
	Body       string   `json:"body"`
	Annotation string   `json:"annotation"`
	FileName   string   `json:"file_name"`
}

const N = 50000

var FictionGenresPrefix = []string{
	"sf", "popadancy", "litrpg", "russian_fantasy", "popadanec",
	"modern_tale", "hronoopera", "child_sf", "love_sf"}

const IgnoreNonFictionProbability = 0.75

var outputCSVPath string
var truncateToNumChars int
var logFilePath string
var zipFilePattern string

// TruncateText truncates text to firstN characters.
func TruncateText(s string, firstN int) string {
	s = strings.TrimSpace(s)
	if len(s) < firstN {
		firstN = len(s)
	}
	return s[:firstN]
}

func ExtractBook(fb2 *zip.File) (Data, error) {
	d := Data{}

	reader, err := fb2.Open()
	if err != nil {
		return d, fmt.Errorf("error opening file %s because %+v", fb2.Name, err)
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return d, err
	}

	enc, name, _ := charset.DetermineEncoding(data, "")
	if name != "utf-8" {
		enc = charmap.Windows1251
	}
	decodedReader := transform.NewReader(bytes.NewReader(data), enc.NewDecoder())
	if err != nil {
		return d, fmt.Errorf("couldn't decode file because of %+v", err)
	}

	xmlText, _ := io.ReadAll(decodedReader)
	doc := soup.HTMLParse(string(xmlText))

	langs := doc.FindAll("lang")
	if len(langs) != 1 || langs[0].Text() != "ru" {
		return d, fmt.Errorf("file is not in Russian")
	}

	d.FileName = fb2.Name
	genres := doc.FindAll("genre")
	if len(genres) == 0 {
		return d, fmt.Errorf("no genres found")
	}

	hasSf := false
	for _, genre := range genres {
		for _, prefix := range FictionGenresPrefix {
			if strings.HasPrefix(genre.Text(), prefix) {
				hasSf = true
				break
			}
		}
	}
	if !hasSf {
		if rand.Float32() < IgnoreNonFictionProbability {
			return d, fmt.Errorf("random ignore")
		}
	}

	d.Genres = make([]string, len(genres))
	for i, genre := range genres {
		d.Genres[i] = genre.Text()
	}
	d.ID = d.FileName

	annotation := doc.FindAll("annotation")
	if len(annotation) == 1 {
		d.Annotation = annotation[0].FullText()
	}

	title := doc.FindAll("book-title")
	if len(title) != 1 {
		return d, fmt.Errorf("error finding book title")
	}
	d.BookTitle = title[0].Text()

	body := doc.FindAll("body")
	if len(body) != 1 {
		return d, fmt.Errorf("error finding body")
	}

	text := body[0].FullText()
	d.Body = TruncateText(text, truncateToNumChars)

	ti := doc.FindAll("title-info")
	if len(ti) != 1 {
		return d, fmt.Errorf("error finding title-info")
	}

	authors := ti[0].FindAll("author")
	if len(authors) == 0 {
		return d, fmt.Errorf("no authors found")
	}

	d.Authors = make([]Author, len(authors))

	for i, author := range authors {
		auth := Author{}

		fn := author.Find("first-name")
		if fn.Error == nil {
			auth.FirstName = fn.Text()
		}

		ln := author.Find("last-name")
		if ln.Error == nil {
			auth.LastName = ln.Text()
		}

		mn := author.Find("middle-name")
		if mn.Error == nil {
			auth.MiddleName = mn.Text()
		}

		nn := author.Find("nickname")
		if nn.Error == nil {
			auth.NickName = nn.Text()
		}

		d.Authors[i] = auth
	}

	return d, nil
}

func expandPatterns(pattern string) ([]string, error) {
	var files []string
	patterns := strings.Split(pattern, ",")

	for _, p := range patterns {
		matches, err := filepath.Glob(p)
		if err != nil {
			return nil, err
		}
		files = append(files, matches...)
	}

	return files, nil
}

func main() {
	flag.StringVar(&outputCSVPath, "output", "", "Output file path")
	flag.IntVar(&truncateToNumChars, "truncate_to", 10000, "Discard first N words")
	flag.StringVar(&logFilePath, "log", "", "Log file path")
	flag.StringVar(&zipFilePattern, "zip_files", "", "Zip file pattern")
	flag.Parse()

	if len(outputCSVPath) < 1 {
		log.Fatal("Output file path is required")
	}

	if len(logFilePath) < 1 {
		log.Fatal("Log file path is required")
	}

	if len(zipFilePattern) < 1 {
		log.Fatal("Zip file pattern is required")
	}

	logFile, err := os.Create(logFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer logFile.Close()

	// Set the log output to the log file
	log.SetOutput(logFile)

	// Set log flags for date and time information
	log.SetFlags(log.Ldate | log.Ltime)

	// Create a new HDF file.
	f, err := os.Create(outputCSVPath)
	if err != nil {
		panic(err)
	}

	const maxGoroutines = 8
	goroutineSem := make(chan struct{}, maxGoroutines)

	var wg sync.WaitGroup

	zipFiles, err := expandPatterns(zipFilePattern)
	if err != nil {
		panic(err)
	}

	log.Println("Found", len(zipFiles), "zip files")

	for n, file := range zipFiles {
		fmt.Println(file)

		r, err := zip.OpenReader(file)
		if err != nil {
			return
		}

		zippedFb2Files := r.File
		fmt.Printf("Processing file %d/%d: %s\n", n+1, len(zipFiles), file)
		bar := pb.New(len(zippedFb2Files))

		for _, fb2 := range zippedFb2Files {
			goroutineSem <- struct{}{} // Wait for an available slot
			wg.Add(1)

			go func(fb2 *zip.File) {
				defer wg.Done()

				d, err := ExtractBook(fb2)
				if err != nil {
					log.Printf("Error extracting book %s/%s: %+v\n", file, fb2.Name, err)
				} else {
					d.ID = fmt.Sprintf("%s/%s", file, d.ID)
					jsdata, err := json.Marshal(d)
					if err != nil {
						log.Printf("Error marshalling data %+v\n", err)
					} else {
						f.Write(jsdata)
						f.WriteString("\n")
						bar.Add(1)
					}
				}

				<-goroutineSem // Release the slot
			}(fb2)
		}

		bar.Finish()
		r.Close()
	}

	wg.Wait() // Wait for all goroutines to complete

	f.Close()
}
