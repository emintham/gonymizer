package gonymizer

import (
	"database/sql"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"strings"
)

// ColumnMaps is an alias to a map from a string to ColumnMapper
type ColumnMaps map[string]ColumnMapper

// normalizeName removes " from names
// Some names may contain quotes if the name is a reserved word. For example tableName public.order would be a
// conflict with ORDER BY so PSQL will add quotes to the name. I.E. public."order". Remove the quotes so we can match
// whatever is in the map file.
func normalizeName(name string) string {
	return strings.Replace(name, "\"", "", -1)
}

// GetKey returns the key that a schema/table/column tuple maps to.
func (maps ColumnMaps) GetKey(tableSchema, tableName, columnName string) string {
	tableSchema = normalizeName(tableSchema)
	tableName = normalizeName(tableName)
	columnName = normalizeName(columnName)

	return fmt.Sprintf("%s|%s|%s", tableSchema, tableName, columnName)
}

// GetColumnMapper returns the ColumnMapper of a given key if it exists.
func (maps ColumnMaps) GetColumnMapper(key string) *ColumnMapper {
	columnMapper, ok := maps[key]
	if !ok {
		return nil
	}

	return &columnMapper
}

// InsertColumnMap inserts a ColumnMapper into ColumnMaps.
func (maps ColumnMaps) InsertColumnMap(mapper ColumnMapper) {
	key := maps.GetKey(mapper.TableSchema, mapper.TableName, mapper.ColumnName)
	maps[key] = mapper
}

// InsertColumnMapIfNotExists inserts a ColumnMapper into ColumnMaps if it does not already exist.
func (maps ColumnMaps) InsertColumnMapIfNotExists(mapper ColumnMapper) {
	key := maps.GetKey(mapper.TableSchema, mapper.TableName, mapper.ColumnName)
	_, ok := maps[key]
	if !ok {
		maps.InsertColumnMap(mapper)
	}
}

// mapColumns
func (maps ColumnMaps) mapColumns(db *sql.DB, schemaPrefix, schema string, excludeTables map[string]bool) error {
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

	if len(schemaPrefix) == 0 && len(schema) == 0 {

		log.Debug("Mapping all schemas")
		rows, err = GetAllSchemaColumns(db)

	} else if len(schemaPrefix) == 0 && len(schema) > 0 {

		log.Debug("Mapping a single schema")
		rows, err = GetSchemaColumnEquals(db, schema)

	} else if schemaPrefix != "" && schema == "" {

		// Invalid
		return errors.New("cannot use SchemaPrefix option without a schema to map it to")

	} else if strings.HasPrefix(schemaPrefix, schema) {

		log.Debug("Mapping a schema with SchemaPrefix present")
		prefixPresent = true
		rows, err = GetSchemaColumnsLike(db, schemaPrefix)

	} else {

		log.Debug("Mapping a single schema")
		rows, err = GetSchemaColumnEquals(db, schema)

	}
	defer rows.Close()

	var (
		tableCatalog    string
		tableSchema     string
		tableName       string
		columnName      string
		dataType        string
		maxLength       int
		ordinalPosition int
		isNullable      bool
	)

	log.Debug("Iterating through rows and creating skeleton map")
	for rows.Next() {
		if rows.Err() != nil {
			log.Fatal(rows.Err())
		}

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
		schemaTableName := fmt.Sprintf("%s.%s", tableSchema, tableName)
		_, ok := excludeTables[schemaTableName]
		if !ok {
			cmap := NewColumnMapper(columnName, tableName, schema, dataType, maxLength, ordinalPosition, isNullable)
			maps.InsertColumnMapIfNotExists(cmap)
		}
	}

	return err
}
