package gonymizer

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

// NewColumnMapper creates a new ColumnMapper
func NewColumnMapper(columnName, tableName, schema, dataType string, maxLength, ordinalPosition int, isNullable bool) ColumnMapper {
	return ColumnMapper{
		Processors: []ProcessorDefinition{
			{
				Name: "Identity",
			},
		},
		TableName:       tableName,
		ColumnName:      columnName,
		DataType:        dataType,
		MaxLength:       maxLength,
		OrdinalPosition: ordinalPosition,
		IsNullable:      isNullable,
		TableSchema:     schema,
	}
}
