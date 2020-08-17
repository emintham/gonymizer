package gonymizer

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
)

// DBMapper is the main structure for the map file JSON object and is used to map all database columns that will be
// anonymized.
type DBMapper struct {
	DBName       string
	SchemaPrefix string
	Seed         int64
	ColumnMaps   ColumnMaps
}

func (m *DBMapper) MarshalJSON() ([]byte, error) {
	type Alias DBMapper

	columnMaps := make([]ColumnMapper, 0, len(m.ColumnMaps))
	for _, v := range m.ColumnMaps {
		columnMaps = append(columnMaps, v)
	}

	return json.Marshal(&struct {
		ColumnMaps []ColumnMapper
		*Alias
	}{
		ColumnMaps: columnMaps,
		Alias:      (*Alias)(m),
	})
}

func (m *DBMapper) UnmarshalJSON(data []byte) error {
	type Alias DBMapper

	aux := &struct {
		ColumnMaps []ColumnMapper
		*Alias
	}{
		Alias: (*Alias)(m),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	m.ColumnMaps = ColumnMaps{}

	for _, cmap := range aux.ColumnMaps {
		m.ColumnMaps.InsertColumnMap(cmap)
	}

	return nil
}

// NewDBMapper will generate a column-map based on the supplied PGConfig and previously configured map file.
func NewDBMapper(conf PGConfig, schemaPrefix string, schemas []string, excludeTables map[string]bool) (*DBMapper, error) {
	db, err := OpenDB(conf)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	dbMapper := DBMapper{
		DBName:       conf.DefaultDBName,
		SchemaPrefix: schemaPrefix,
	}
	dbMapper.mapSchemas(db, schemas, schemaPrefix, excludeTables)

	return &dbMapper, nil
}

// NewDBMapperFromFile will load the column-map into memory for use in dumping, processing, and loading of SQL files.
func NewDBMapperFromFile(givenPathToFile string) (*DBMapper, error) {
	pathToFile := givenPathToFile

	f, err := os.Open(pathToFile)
	if err != nil {
		log.Error("Failure to open file: ", err)
		log.Error("givenPathToFile: ", givenPathToFile)
		log.Error("pathToFile: ", pathToFile)
		return nil, err
	}
	defer f.Close()

	jsonDecoder := json.NewDecoder(f)

	dbMapper := new(DBMapper)
	err = jsonDecoder.Decode(dbMapper)
	if err != nil {
		log.Error(err)
		log.Error("givenPathToFile: ", givenPathToFile)
		log.Error("pathToFile: ", pathToFile)
		log.Error("f: ", f)
		return nil, err
	}

	err = dbMapper.Validate()
	if err != nil {
		log.Error(err)
		log.Error("dbMapper: ", dbMapper)
		return nil, err
	}

	return dbMapper, nil
}

// ColumnMapper returns the address of the ColumnMapper object if it matches the given parameters otherwise it returns
// nil. Special cases exist for sharded schemas using the schema-prefix. See documentation for details.
func (m *DBMapper) ColumnMapper(schemaName, tableName, columnName string) *ColumnMapper {
	key := m.ColumnMaps.GetKey(schemaName, tableName, columnName)
	return m.ColumnMaps.GetColumnMapper(key)
}

// WriteToFile will save the supplied DBMap to filepath.
func (m *DBMapper) WriteToFile(filepath string) error {
	f, err := os.Create(filepath)
	if err != nil {
		log.Error("Failure to open file: ", err)
		log.Error("filepath: ", filepath)
		return err
	}
	defer f.Close()

	jsonEncoder := json.NewEncoder(f)
	jsonEncoder.SetIndent("", "    ")

	err = jsonEncoder.Encode(m)
	if err != nil {
		log.Error(err)
		log.Error("filepath", filepath)
		return err
	}

	return nil
}

// mapSchemas maps schemas to anonymizing processors.
func (m *DBMapper) mapSchemas(db *sql.DB, schemas []string, schemaPrefix string, excludeTables map[string]bool) error {
	m.ColumnMaps = ColumnMaps{}

	if len(schemas) < 1 {
		schemas = append(schemas, "public")
	}

	log.Info("Schemas to map: ", schemas)
	for _, schema := range schemas {
		log.Info("Mapping columns for schema: ", schema)
		if err := m.ColumnMaps.mapColumns(db, schemaPrefix, schema, excludeTables); err != nil {
			return err
		}
	}

	return nil
}

// Validate is used to verify that a database map is complete and correct.
func (m *DBMapper) Validate() error {
	if len(m.DBName) == 0 {
		return errors.New("expected non-empty DBName")
	}

	// Ensure that each processor is defined
	for _, columnMap := range m.ColumnMaps {
		for _, processor := range columnMap.Processors {
			if _, ok := ProcessorCatalog[processor.Name]; !ok {
				return fmt.Errorf("Unrecognized Processor %s", processor.Name)
			}
		}
	}
	return nil
}
