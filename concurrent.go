package gonymizer

import (
	"bufio"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"unicode"
)

// maxLinesPerChunk bounds the number of lines per chunk.
const maxLinesPerChunk = 100000

// Chunk is a section of Postgres' data dump together with metadata.
type Chunk struct {
	Data        *strings.Builder
	SchemaName  string
	TableName   string
	ColumnNames []string
	ChunkNumber int
	// SubChunkNumber is usually 0, but if data is split across multiple chunks for the same schema, the ChunkNumber
	// will be the same across each chunk but with SubChunkNumbers in increasing order.
	SubChunkNumber int
	// number of lines included in the chunk
	NumLines int
	// the line number starting from which actual table data is defined.
	DataBegins int
}

// Filename returns a filename for a chunk.
func (c Chunk) Filename() string {
	return fmt.Sprintf("%06d.%06d.part", c.ChunkNumber, c.SubChunkNumber)
}

// ProcessConcurrently will process the supplied dump file concurrently according to the supplied database map file.
// generateSeed can also be set to true which will inform the function to use Go's built-in random number generator.
func ProcessConcurrently(mapper *DBMapper, src, dst string, inclusive, generateSeed bool, numWorkers int, preProcessFile, postProcessFile string) error {
	err := seedRNG(mapper, generateSeed)
	if err != nil {
		return err
	}

	srcFile, err := os.Open(src)
	if err != nil {
		log.Error(err)
		return err
	}
	defer srcFile.Close()

	var wg sync.WaitGroup
	chunks := make(chan Chunk, numWorkers*2)
	startChunkWorkers(mapper, &wg, numWorkers, chunks, inclusive)

	reader := bufio.NewReader(srcFile)

	go createChunks(chunks, reader, &wg)

	wg.Wait()

	mergeFiles(dst, preProcessFile, postProcessFile)

	return nil
}

// startChunkWorkers starts a number of workers to process chunks when they arrive in a given channel
func startChunkWorkers(mapper *DBMapper, wg *sync.WaitGroup, numWorkers int, chunks <-chan Chunk, inclusive bool) {
	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go startChunkWorker(chunks, wg, mapper, inclusive)
		log.Infof("Worker %d started!", i+1)
	}
}

// createChunks takes a reader and splits it up into roughly maxLinesPerChunk sized pieces. These pieces are sent
// through a channel.
func createChunks(chunks chan<- Chunk, reader *bufio.Reader, wg *sync.WaitGroup) {
	defer close(chunks)

	defer wg.Done()
	wg.Add(1)

	var (
		schemaName    string
		tableName     string
		columnNames   []string
		chunkCount    int
		subchunkCount int
		eof           bool
		hasSubchunk   bool
	)

	for !eof {
		var (
			builder  strings.Builder
			numLines int
		)

		chunk := Chunk{
			Data:           &builder,
			ChunkNumber:    chunkCount,
			SubChunkNumber: subchunkCount,
			SchemaName:     schemaName,
			TableName:      tableName,
			ColumnNames:    columnNames,
		}

		for numLines = 1; numLines < maxLinesPerChunk; numLines++ {
			input, err := reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					eof = true
				} else {
					log.Fatal(err)
				}
				break
			}

			builder.WriteString(input)

			trimmedInput := strings.TrimLeftFunc(input, unicode.IsSpace)
			if strings.HasPrefix(trimmedInput, StateChangeTokenBeginCopy) {
				pattern := `^COPY (?P<Schema>[a-zA-Z_]+)\.(?P<TableName>\w+) \((?P<Columns>.*)\) .*`
				r := regexp.MustCompile(pattern)
				submatch := r.FindStringSubmatch(trimmedInput)
				if len(submatch) == 0 {
					log.Fatal("Regex doesn't match: ", trimmedInput)
				}

				schemaName = submatch[1]
				tableName = submatch[2]
				columnNames = strings.Split(submatch[3], ", ")

				chunk.SchemaName = schemaName
				chunk.TableName = tableName
				chunk.ColumnNames = columnNames
				chunk.DataBegins = numLines

				hasSubchunk = true
			}

			if strings.HasPrefix(trimmedInput, StateChangeTokenEndCopy) {
				subchunkCount = 0
				schemaName = ""
				tableName = ""
				columnNames = nil
				hasSubchunk = false
				break
			}

			if numLines == maxLinesPerChunk {
				if hasSubchunk {
					subchunkCount++
				}
			}
		}

		chunk.NumLines = numLines
		chunks <- chunk

		if subchunkCount == 0 {
			chunkCount++
		}
	}

	log.Infof("Processed %d chunks", chunkCount-1)
}

// mergeFiles takes a destination filename and pre/post-process files and writes all part files to the destination file
// including any pre/post-processing file data.
func mergeFiles(dst string, preProcessFile, postProcessFile string) error {
	log.Info("Merging partial files...")

	dstFile, err := os.Create(dst)
	defer dstFile.Close()

	if err != nil {
		log.Error(err)
		return err
	}

	if len(preProcessFile) > 0 {
		if err = fileInjector(preProcessFile, dstFile); err != nil {
			log.Error("Unable to run preProcessor")
			return err
		}
	}

	if _, err := dstFile.WriteString("SET session_replication_role = 'replica';\n"); err != nil {
		return err
	}

	pwd, err := os.Getwd()
	if err != nil {
		return err
	}

	matches, err := filepath.Glob("*.part")
	if err != nil {
		return err
	}

	for _, filename := range matches {
		path := filepath.Join(pwd, filename)
		partFile, err := os.Open(path)
		if err != nil {
			log.Error(err)
			return err
		}

		_, err = io.Copy(dstFile, partFile)
		if err != nil {
			log.Error(err)
			return err
		}

		partFile.Close()
		os.Remove(filename)
	}

	if len(postProcessFile) > 0 {
		if err = fileInjector(postProcessFile, dstFile); err != nil {
			return err
		}
	}

	if _, err := dstFile.WriteString("SET session_replication_role = 'origin';\n"); err != nil {
		return err
	}

	return nil
}

// startChunkWorkers takes a receive-only channel of chunks and processes each chunk, writing them to file.
// startChunkWorker takes a receive-only channel of chunks and processes each chunk, writing them to file.
func startChunkWorker(chunks <-chan Chunk, wg *sync.WaitGroup, mapper *DBMapper, inclusive bool) {
	defer wg.Done()

	for chunk := range chunks {
		dst := chunk.Filename()

		dstFile, err := os.Create(dst)
		if err != nil {
			log.Fatal(err)
		}

		// This is weird... is this the best way to do this?
		reader := bufio.NewReader(strings.NewReader(chunk.Data.String()))

		for i := 0; i > -1; i++ {
			input, err := reader.ReadString('\n')
			if err != nil {
				if err != io.EOF {
					log.Fatal(err)
				}
				break
			}

			trimmedInput := strings.TrimLeftFunc(input, unicode.IsSpace)
			isEnd := strings.HasPrefix(trimmedInput, StateChangeTokenEndCopy)
			isEmpty := len(trimmedInput) == 0
			aboveData := i < chunk.DataBegins
			afterAllData := i > chunk.DataBegins && chunk.SchemaName == ""

			if aboveData || isEnd || isEmpty || afterAllData {
				dstFile.WriteString(input)
				continue
			}

			output := processRowFromChunk(mapper, input, chunk.SchemaName, chunk.TableName, chunk.ColumnNames, inclusive)
			dstFile.WriteString(output)
		}

		dstFile.Close()

		log.Infof("%s written to file", dst)
	}
}

// processRowFromChunk processes a data row from a chunk
func processRowFromChunk(mapper *DBMapper, inputLine, schemaName, tableName string, columnNames []string, inclusive bool) string {
	rowVals := strings.Split(inputLine, "\t")
	outputVals := make([]string, 0, len(rowVals))

	for i, columnName := range columnNames {
		var (
			err        error
			escapeChar string
			output     string
		)

		cmap := mapper.ColumnMapper(schemaName, tableName, columnName)
		if cmap == nil && inclusive {
			log.Fatalf("Column '%s.%s.%s' does not exist. Please add to Map file",
				schemaName, tableName, columnName)
			os.Exit(1)
		}
		val := rowVals[i]

		// Check to see if the column has an escape char at the end of it.
		// If so cut it and keep it for later
		if strings.HasSuffix(val, "\n") {
			escapeChar = "\n"
			val = strings.Replace(val, "\n", "", -1)
		} else if strings.HasSuffix(val, "\t") {
			escapeChar = "\t"
			val = strings.Replace(val, "\t", "", -1)
		}

		// If column value is nil or if this column is not mapped, keep the value and continue on
		if val == "\\N" || cmap == nil {
			output = val
		} else {
			output, err = processValue(cmap, val)
			if err != nil {
				log.Error(err)
				log.Debug("i: ", i)
				log.Debug("columnName: ", columnName)
				os.Exit(1)
			}
		}
		// Add escape character back to column
		output += escapeChar

		// Append the column to our new line
		outputVals = append(outputVals, output)
	}

	return strings.Join(outputVals, "\t")
}
