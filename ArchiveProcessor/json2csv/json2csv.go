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

const maxCapacity = 5 * 1024 * 1024 * 1024

var input string
var output string
var discardFirstWords int
var useFiles bool

func getFiles() []string {
	files := []string{"753030.fb2", "753029.fb2", "752121.fb2", "497148.fb2", "501398.fb2", "752122.fb2", "752120.fb2", "552329.fb2", "753031.fb2", "753032.fb2", "755720.fb2", "644955.fb2", "755721.fb2", "710622.fb2", "711519.fb2", "752119.fb2", "753028.fb2", "755719.fb2", "532471.fb2", "450323.fb2", "450324.fb2", "450325.fb2", "553516.fb2", "556597.fb2", "636576.fb2", "450327.fb2", "619971.fb2", "582607.fb2", "693349.fb2", "693361.fb2", "693346.fb2", "477565.fb2", "476978.fb2", "475108.fb2", "693348.fb2", "478622.fb2", "693359.fb2", "693344.fb2", "693347.fb2", "743384.fb2", "741717.fb2", "590118.fb2", "554672.fb2", "554770.fb2", "554769.fb2", "566760.fb2", "588453.fb2", "648325.fb2", "533343.fb2", "591540.fb2", "608475.fb2", "552981.fb2", "539817.fb2", "552979.fb2", "622698.fb2", "664507.fb2", "556768.fb2", "564146.fb2", "577584.fb2", "599185.fb2", "616366.fb2", "617347.fb2", "637845.fb2", "648854.fb2", "691559.fb2", "754783.fb2", "548574.fb2", "525064.fb2", "564339.fb2", "586199.fb2", "622456.fb2", "657391.fb2", "730122.fb2", "577776.fb2", "600436.fb2", "600437.fb2", "601422.fb2", "609214.fb2", "609585.fb2", "623736.fb2", "630100.fb2", "660951.fb2", "678314.fb2", "678312.fb2", "706500.fb2", "740257.fb2", "583533.fb2", "582971.fb2", "585998.fb2", "588150.fb2", "591195.fb2", "597454.fb2", "602185.fb2", "603839.fb2", "606680.fb2", "616633.fb2", "637499.fb2", "676093.fb2", "600900.fb2", "616092.fb2", "608919.fb2", "622817.fb2", "634955.fb2", "643862.fb2", "750497.fb2", "665066.fb2", "665068.fb2", "613855.fb2", "665154.fb2", "665155.fb2", "665210.fb2", "665069.fb2", "665052.fb2", "665153.fb2", "665209.fb2", "640360.fb2", "660091.fb2", "647975.fb2", "692823.fb2", "693301.fb2", "726412.fb2", "726413.fb2", "695574.fb2", "676143.fb2", "693509.fb2", "676873.fb2", "693510.fb2", "711465.fb2", "733035.fb2", "701371.fb2", "740051.fb2", "743577.fb2", "692730.fb2", "692731.fb2", "703519.fb2", "708175.fb2", "710418.fb2", "711534.fb2", "722711.fb2", "724275.fb2", "732302.fb2", "735681.fb2", "747817.fb2", "750426.fb2", "754174.fb2", "754194.fb2", "732545.fb2", "746165.fb2", "732270.fb2", "732269.fb2", "730317.fb2", "732268.fb2", "743387.fb2", "743386.fb2", "758399.fb2", "707015.fb2", "713278.fb2", "726235.fb2", "746408.fb2", "746409.fb2", "750064.fb2", "749724.fb2", "756178.fb2"}

	return files
}

func main() {
	flag.StringVar(&input, "input", "", "Input file path")
	flag.StringVar(&output, "output", "", "Output file path")
	flag.IntVar(&discardFirstWords, "discard_first_words", 0,
		"Number of first words to discard from body")
	flag.BoolVar(&useFiles, "use_files", true, "Use files for custom labelling")
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

	selectedFiles := getFiles()

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
		for _, v := range selectedFiles {
			if v == fileName {
				print("Selected: " + fileName + "\n")
				isSelected = "1"
				break
			}
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

		err = writer.Write([]string{
			bodyWithAnnotation,
			strings.Join(genre, ","),
			isSelected,
			fileName}) // Write the modified "body" with "annotation" and "genre"

		if err != nil {
			panic(err)
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}
}
