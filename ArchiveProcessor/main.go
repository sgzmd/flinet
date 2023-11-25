package main

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/anaskhan96/soup"
	pb "github.com/schollz/progressbar/v3"
	"golang.org/x/net/html/charset"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type Author struct {
	FirstName  string `json:"first_name"`
	LastName   string `json:"last_name"`
	MiddleName string `json:"middle_name"`
	NickName   string `json:"nick_name"`
}

type Data struct {
	ID        string   `json:"id"`
	Genres    []string `json:"genre"`
	Authors   []Author `json:"author"`
	BookTitle string   `json:"book_title"`
	Body      string   `json:"body"`
	FileName  string   `json:"file_name"`
}

const N = 2000

func normalize(text string) string {
	// Convert text to NFC form
	return norm.NFC.String(text)
}

func tokenize(text string) []string {
	// Regular expression to match words
	// This regex might need refinement based on your specific needs
	wordRegexp := regexp.MustCompile(`\p{L}+`)

	return wordRegexp.FindAllString(text, -1)
}

func removePunctuation(text string) string {
	// Regular expression to match punctuation
	punctuationRegexp := regexp.MustCompile(`[\p{P}\p{S}]`)

	return punctuationRegexp.ReplaceAllString(text, "")
}

func TokenizeAndStemText(s string, firstN int) string {
	norm := normalize(s)
	dp := removePunctuation(norm)
	fields := tokenize(dp)

	if len(fields) < firstN {
		firstN = len(fields)
	}
	tokens := fields[:firstN]
	return strings.Join(tokens, " ")
}

// TruncateText truncates text to firstN characters.
func TruncateText(s string, firstN int) string {
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

	d.Genres = make([]string, len(genres))
	for i, genre := range genres {
		d.Genres[i] = genre.Text()
	}
	d.ID = d.FileName

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

	zipFiles, err := filepath.Glob(filepath.Join(dir, "*.zip"))
	if err != nil {
		panic(err)
	}

	log.Println("Found", len(zipFiles), "zip files")

	// Iterate over the files in the dsirectory.
	for _, file := range zipFiles {
		fmt.Println(file)

		r, err := zip.OpenReader(file)
		if err != nil {
			return
		}

		zippedFb2Files := r.File

		fmt.Printf("Reading data from %s\n", file)
		bar := pb.New(len(zippedFb2Files)) // Set the total count of the progress bar
		for _, fb2 := range zippedFb2Files {
			d, err := ExtractBook(fb2)
			if err != nil {
				log.Printf("Error extracting book %s/%s: %+v\n", file, fb2.Name, err)
				continue // or continue, or anything else
			}
			d.ID = fmt.Sprintf("%s/%s", file, d.ID)

			jsdata, err := json.Marshal(d)
			if err != nil {
				log.Printf("Error marshalling data %+v\n", err)
				continue
			}

			f.Write(jsdata)
			f.WriteString("\n")
			bar.Add(1)
		}

		// Close the progress bar.
		bar.Finish()
	}

	// Close the HDF file.
	f.Close()
}
