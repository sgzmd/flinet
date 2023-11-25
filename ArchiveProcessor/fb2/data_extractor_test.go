package fb2

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnmarshallBook(t *testing.T) {
	// read entire testbook.xml
	data, err := ioutil.ReadFile("testbook.xml")
	assert.NoError(t, err, "Test case 1 failed: unexpected error")
	book, err := ParseFictionBook(data)
	assert.NoError(t, err, "Test case 1 failed: unexpected error")
	// Assert the expected values
	assert.Equal(t, "Второе рождение Венеры", book.Description.TitleInfo.BookTitle, "Test case 1 failed: unexpected Title")
	assert.Equal(t, 2, len(book.Body.Sections), "Test case 1 failed: unexpected number of sections")
	assert.Equal(t, 2, len(book.Body.Sections[1].Content), "Test case 1 failed: unexpected number of sections")
	assert.Regexp(t, "На островах Мадейры всегда хорошая погода. "+
		"Эти острова находятся гораздо ближе к Африканскому континенту",
		book.Body.Sections[1].Content[0], "Test case 1 failed: unexpected section title")

	// Sequences should be seq1 and seq2
	seqs := []Sequence{{Name: "seq1"}, {Name: "seq2"}}
	assert.Equal(t, seqs, book.Description.TitleInfo.Sequences, "Test case 1 failed: unexpected sequences")
}
