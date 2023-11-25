package fb2

import (
	"encoding/xml"
	"log"
	"strings"
)

type FictionBook struct {
	XMLName     xml.Name    `xml:"FictionBook"`
	Description Description `xml:"description"`
	Body        Body        `xml:"body"`
}

type Description struct {
	TitleInfo    TitleInfo    `xml:"title-info"`
	DocumentInfo DocumentInfo `xml:"document-info"`
	PublishInfo  PublishInfo  `xml:"publish-info"`
}

type TitleInfo struct {
	Genres     []string   `xml:"genre"`
	Authors    []Author   `xml:"author"`
	BookTitle  string     `xml:"book-title"`
	Annotation Annotation `xml:"annotation"`
	Date       string     `xml:"date"`
	CoverPage  CoverPage  `xml:"coverpage"`
	Lang       string     `xml:"lang"`
	Sequences  []Sequence `xml:"sequence"`
}

type Author struct {
	FirstName  string `xml:"first-name"`
	MiddleName string `xml:"middle-name"`
	LastName   string `xml:"last-name"`
	NickName   string `xml:"nick-name"`
	ID         string `xml:"id"`
}

type Annotation struct {
	Content string `xml:"p"`
}

type CoverPage struct {
	Image Image `xml:"image"`
}

type Image struct {
	Href string `xml:"l:href,attr"`
}

type Sequence struct {
	Name string `xml:"name,attr"`
}

type DocumentInfo struct {
	Author      Author  `xml:"author"`
	ProgramUsed string  `xml:"program-used"`
	Date        string  `xml:"date"`
	SrcURL      string  `xml:"src-url"`
	SrcOCR      string  `xml:"src-ocr"`
	ID          string  `xml:"id"`
	Version     string  `xml:"version"`
	History     History `xml:"history"`
}

type History struct {
	Content string `xml:"p"`
}

type PublishInfo struct {
	BookName  string `xml:"book-name"`
	Publisher string `xml:"publisher"`
	City      string `xml:"city"`
	Year      string `xml:"year"`
	ISBN      string `xml:"isbn"`
}

type Body struct {
	Title    Title     `xml:"title"`
	Sections []Section `xml:"section"`
}

type Title struct {
	Content []string `xml:"p"`
}

type Section struct {
	Title   Title    `xml:"title"`
	Content []string `xml:"p"`
}

type FlattenedBook struct {
	Title      string
	Author     []string
	Annotation string
	Sequences  []string
	Genres     []string
	Content    string
}

func ParseFictionBook(data []byte) (*FictionBook, error) {
	book := &FictionBook{}
	err := xml.Unmarshal(data, &book)

	if err != nil {
		log.Printf("Error unmarshalling book: %+v", err)
		return nil, err
	}

	return book, nil
}

func (book *FictionBook) Flatten() *FlattenedBook {
	flattened := &FlattenedBook{
		Title:      book.Description.TitleInfo.BookTitle,
		Annotation: book.Description.TitleInfo.Annotation.Content,
		Sequences:  make([]string, len(book.Description.TitleInfo.Sequences)),
		Genres:     book.Description.TitleInfo.Genres,
		Content:    "",
	}

	for i, seq := range book.Description.TitleInfo.Sequences {
		flattened.Sequences[i] = seq.Name
	}

	for _, section := range book.Body.Sections {
		if len(section.Title.Content) > 0 {
			flattened.Content += section.Title.Content[0] + "\n"
		}

		for _, p := range section.Content {
			flattened.Content += p + "\n"
		}
	}

	// Remove all new lines
	flattened.Content = strings.Replace(flattened.Content, "\n", "", -1)
	if len(flattened.Content) > 5000 {
		flattened.Content = flattened.Content[:5000]
	}

	// Flatten authors
	for _, author := range book.Description.TitleInfo.Authors {
		flattened.Author = append(flattened.Author, author.FirstName+" "+author.MiddleName+" "+author.LastName)
	}

	return flattened
}
