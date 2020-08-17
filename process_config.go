package gonymizer

// ProcessConfig contains  the data required to process a dump file concurrently.
type ProcessConfig struct {
	DBMapper            *DBMapper
	SourceFilename      string
	DestinationFilename string
	MapFilename         string
	Inclusive           bool
	GenerateSeed        bool
	NumWorkers          int
	PreprocessFilename  string
	PostprocessFilename string
}
