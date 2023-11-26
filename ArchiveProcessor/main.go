package main

import (
	"archive/zip"
	"bytes"
	"encoding/json"
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
		if strings.HasPrefix(genre.Text(), "sf") {
			hasSf = true
			break
		} else if genre.Text() == "popadanec" || genre.Text() == "litrpg" {
			hasSf = true
			break
		}
	}
	if !hasSf {
		if rand.Float32() < 0.75 {
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
	d.Body = TruncateText(text, N)

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

func main() {
	logFile, err := os.Create("app.log")
	if err != nil {
		log.Fatal(err)
	}
	defer logFile.Close()

	// Set the log output to the log file
	log.SetOutput(logFile)

	// Set log flags for date and time information
	log.SetFlags(log.Ldate | log.Ltime)

	// Get the path to the directory with zip files.
	dir := os.Args[1]

	// Create a new HDF file.
	f, err := os.Create("data.json")
	if err != nil {
		panic(err)
	}

	const maxGoroutines = 8
	goroutineSem := make(chan struct{}, maxGoroutines)

	var wg sync.WaitGroup

	zipFiles, err := filepath.Glob(filepath.Join(dir, "*.zip"))
	if err != nil {
		panic(err)
	}

	log.Println("Found", len(zipFiles), "zip files")

	for _, file := range zipFiles {
		fmt.Println(file)

		r, err := zip.OpenReader(file)
		if err != nil {
			return
		}

		zippedFb2Files := r.File
		fmt.Printf("Reading data from %s\n", file)
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
