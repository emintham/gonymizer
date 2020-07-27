package gonymizer

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
)

// ProcessorDefinition is the processor data structure used to map database columns to their specified column processor.
type ProcessorDefinition struct {
	Name string

	// optional helpers
	Max      float64
	Min      float64
	Variance float64

	Comment string
}

// ColumnMapper is the data structure that contains all gonymizer required information for the specified column.
type ColumnMapper struct {
	Comment         string
	TableSchema     string
	TableName       string
	ColumnName      string
	DataType        string
	MaxLength       int
	ParentSchema    string
	ParentTable     string
	ParentColumn    string
	OrdinalPosition int

	IsNullable bool

	Processors []ProcessorDefinition
}

// DBMapper is the main structure for the map file JSON object and is used to map all database columns that will be
// anonymized.
type DBMapper struct {
	DBName          string
	SchemaPrefix    string
	Seed            int64
	ColumnMapsAsMap map[string]ColumnMapper `json:"-"`
	ColumnMaps      []ColumnMapper
}

func (m *DBMapper) PopulateColumnMaps() {
	m.ColumnMaps = make([]ColumnMapper, 0, len(m.ColumnMapsAsMap))
	for _, v := range m.ColumnMapsAsMap {
		m.ColumnMaps = append(m.ColumnMaps, v)
	}
}

func (m *DBMapper) PopulateColumnMapsAsMap() {
	m.ColumnMapsAsMap = map[string]ColumnMapper{}

	for _, cmap := range m.ColumnMaps {
		key := fmt.Sprintf("%s|%s|%s", cmap.TableSchema, cmap.TableName, cmap.ColumnName)
		m.ColumnMapsAsMap[key] = cmap
	}
}

// normalizeName removes " from names
// Some names may contain quotes if the name is a reserved word. For example tableName public.order would be a
// conflict with ORDER BY so PSQL will add quotes to the name. I.E. public."order". Remove the quotes so we can match
// whatever is in the map file.
func normalizeName(name string) string {
	return strings.Replace(name, "\"", "", -1)
}

// ColumnMapper returns the address of the ColumnMapper object if it matches the given parameters otherwise it returns
// nil. Special cases exist for sharded schemas using the schema-prefix. See documentation for details.
func (dbMap DBMapper) ColumnMapper(schemaName, tableName, columnName string) *ColumnMapper {

	schemaName = normalizeName(schemaName)
	tableName = normalizeName(tableName)
	columnName = normalizeName(columnName)

	key := fmt.Sprintf("%s|%s|%s", schemaName, tableName, columnName)
	columnMapper, ok := dbMap.ColumnMapsAsMap[key]
	if !ok {
		return nil
	}

	return &columnMapper
}

// Validate is used to verify that a database map is complete and correct.
func (dbMap *DBMapper) Validate() error {
	if len(dbMap.DBName) == 0 {
		return errors.New("expected non-empty DBName")
	}
	// Ensure that each processor is defined
	for _, columnMap := range dbMap.ColumnMapsAsMap {
		for _, processor := range columnMap.Processors {
			if _, ok := ProcessorCatalog[processor.Name]; !ok {
				return fmt.Errorf("Unrecognized Processor %s", processor.Name)
			}
		}
	}
	return nil
}

// GenerateConfigSkeleton will generate a column-map based on the supplied PGConfig and previously configured map file.
func GenerateConfigSkeleton(conf PGConfig, schemaPrefix string, schemas, excludeTables []string) (*DBMapper, error) {
	db, err := OpenDB(conf)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	dbMapper := new(DBMapper)
	dbMapper.DBName = conf.DefaultDBName
	dbMapper.SchemaPrefix = schemaPrefix

	columnMap := map[string]ColumnMapper{}

	if len(schemas) < 1 {
		schemas = append(schemas, "public")
	}

	log.Info("Schemas to map: ", schemas)
	for _, schema := range schemas {
		log.Info("Mapping columns for schema: ", schema)
		columnMap, err = mapColumns(db, columnMap, schemaPrefix, schema, excludeTables)
		if err != nil {
			return nil, err
		}
	}
	dbMapper.ColumnMapsAsMap = columnMap
	return dbMapper, nil
}

// WriteConfigSkeleton will save the supplied DBMap to filepath.
func WriteConfigSkeleton(dbMapper *DBMapper, filepath string) error {

	f, err := os.Create(filepath)
	if err != nil {
		log.Error("Failure to open file: ", err)
		log.Error("filepath: ", filepath)
		return err
	}
	defer f.Close()

	jsonEncoder := json.NewEncoder(f)
	jsonEncoder.SetIndent("", "    ")

	dbMapper.PopulateColumnMaps()
	err = jsonEncoder.Encode(dbMapper)
	if err != nil {
		log.Error(err)
		log.Error("filepath", filepath)
		return err
	}

	return nil
}

// LoadConfigSkeleton will load the column-map into memory for use in dumping, processing, and loading of SQL files.
func LoadConfigSkeleton(givenPathToFile string) (*DBMapper, error) {
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

	dbMapper.PopulateColumnMapsAsMap()
	err = dbMapper.Validate()
	if err != nil {
		log.Error(err)
		log.Error("dbMapper: ", dbMapper)
		return nil, err
	}

	return dbMapper, nil
}

// addColumnIfNotExists searches for columnName in columns, if the column exists in the dbMapper leave as-is otherwise create a
// new one and add to the column map. Returns true if a column map was added
func addColumnIfNotExists(columns map[string]ColumnMapper, columnName, tableName, schemaPrefix, schema, dataType string, maxLength, ordinalPosition int, isNullable bool) bool {
	key := fmt.Sprintf("%s|%s|%s", schema, tableName, columnName)
	_, ok := columns[key]
	if !ok {
		key = fmt.Sprintf("%s|%s|%s", schema, tableName, columnName)
		columns[key] = NewColumnMapper(columnName, tableName, schema, dataType, maxLength, ordinalPosition, isNullable)
		return true
	}

	return false
}

// NewColumnMapper creates a new ColumnMapper
func NewColumnMapper(columnName, tableName, schema, dataType string, maxLength, ordinalPosition int,
	isNullable bool) ColumnMapper {
	col := ColumnMapper{}

	col.Processors = []ProcessorDefinition{
		{
			Name: "Identity",
		},
	}
	col.TableName = tableName
	col.ColumnName = columnName
	col.DataType = dataType
	col.MaxLength = maxLength
	col.OrdinalPosition = ordinalPosition
	col.IsNullable = isNullable
	col.TableSchema = schema

	return col
}

// mapColumns
func mapColumns(db *sql.DB, columns map[string]ColumnMapper, schemaPrefix, schema string,
	excludeTables []string) (map[string]ColumnMapper, error) {
	var (
		err           error
		rows          *sql.Rows
		prefixPresent bool
	)

	// Below is a high level state diagram based on the schema prefix and schema being supplied.
	// empty = empty string or ""
	// group_ = example schema prefix
	// public = example schema name
	// =====================================================
	//  Shard  | Schema | Outcome
	// -----------------------------------------------------
	//  empty  |  empty | Map All Schemas
	// -----------------------------------------------------
	//  empty  | public | Map only provided schema name
	// -----------------------------------------------------
	//  group_ |  empty | Invalid
	// -----------------------------------------------------
	//  group_ |  group | build single map for schema prefix
	// -----------------------------------------------------
	//  group_ | public | Map only provided schema
	// -----------------------------------------------------
	prefixPresent = false

	if len(schemaPrefix) == 0 && len(schema) == 0 {

		log.Debug("Mapping all schemas")
		rows, err = GetAllSchemaColumns(db)

	} else if len(schemaPrefix) == 0 && len(schema) > 0 {

		log.Debug("Mapping a single schema")
		rows, err = GetSchemaColumnEquals(db, schema)

	} else if schemaPrefix != "" && schema == "" {

		// Invalid
		return nil, errors.New("cannot use SchemaPrefix option without a schema to map it to")

	} else if strings.HasPrefix(schemaPrefix, schema) {

		log.Debug("Mapping a schema with SchemaPrefix present")
		prefixPresent = true
		rows, err = GetSchemaColumnsLike(db, schemaPrefix)

	} else {

		log.Debug("Mapping a single schema")
		rows, err = GetSchemaColumnEquals(db, schema)

	}
	defer rows.Close()

	log.Debug("Iterating through rows and creating skeleton map")
	for {
		var (
			tableCatalog    string
			tableSchema     string
			tableName       string
			columnName      string
			dataType        string
			maxLength       int
			ordinalPosition int
			isNullable      bool
			exclude         bool
		)

		// Iterate through each row and add the columns
		for rows.Next() {
			err = rows.Scan(
				&tableCatalog,
				&tableSchema,
				&tableName,
				&columnName,
				&dataType,
				&maxLength,
				&ordinalPosition,
				&isNullable,
			)

			// If we are working on a schema prefix, make sure to use the schema prefix + * as a name, otherwise empty
			if prefixPresent {
				tableSchema = schemaPrefix + "*"
			} else {
				schemaPrefix = ""
			}

			// check to see if table is in the list of skipped tables or data for the table (leave them out of map)
			exclude = false
			for _, item := range excludeTables {
				schemaTableName := fmt.Sprintf("%s.%s", tableSchema, tableName)
				if schemaTableName == item {
					exclude = true
					break
				}
			}
			if exclude {
				continue
			}

			addColumnIfNotExists(columns, columnName, tableName, schemaPrefix, schema, dataType, maxLength, ordinalPosition, isNullable)
		}

		if !rows.NextResultSet() {
			break
		}
	}

	return columns, err
}
