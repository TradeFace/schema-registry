package main

import (
	"encoding/json"
	"fmt"
)

type JSONSchema struct {
	Type                 string                 `json:"type,omitempty"`
	Items                *JSONSchema            `json:"items,omitempty"`
	Properties           map[string]*JSONSchema `json:"properties,omitempty"`
	Required             []string               `json:"required,omitempty"`
	AdditionalProperties *JSONSchema            `json:"additionalProperties,omitempty"`
	Definitions          map[string]*JSONSchema `json:"definitions,omitempty"`
	Enum                 []interface{}          `json:"enum,omitempty"`
	Format               string                 `json:"format,omitempty"`
	Pattern              string                 `json:"pattern,omitempty"`
	Maximum              *float64               `json:"maximum,omitempty"`
	ExclusiveMaximum     *float64               `json:"exclusiveMaximum,omitempty"`
	Minimum              *float64               `json:"minimum,omitempty"`
	ExclusiveMinimum     *float64               `json:"exclusiveMinimum,omitempty"`
	MaxLength            *int                   `json:"maxLength,omitempty"`
	MinLength            *int                   `json:"minLength,omitempty"`
	MultipleOf           *float64               `json:"multipleOf,omitempty"`
	MaxItems             *int                   `json:"maxItems,omitempty"`
	MinItems             *int                   `json:"minItems,omitempty"`
	UniqueItems          bool                   `json:"uniqueItems,omitempty"`
	Ref                  string                 `json:"$ref,omitempty"`
	OneOf                []*JSONSchema          `json:"oneOf,omitempty"`
	AnyOf                []*JSONSchema          `json:"anyOf,omitempty"`
	AllOf                []*JSONSchema          `json:"allOf,omitempty"`
	Not                  *JSONSchema            `json:"not,omitempty"`
	Title                string                 `json:"title,omitempty"`
	Description          string                 `json:"description,omitempty"`
	Default              interface{}            `json:"default,omitempty"`
	AdditionalItems      *JSONSchema            `json:"additionalItems,omitempty"`
	ReadOnly             bool                   `json:"readOnly,omitempty"`
	WriteOnly            bool                   `json:"writeOnly,omitempty"`
	Examples             []interface{}          `json:"examples,omitempty"`
	If                   *JSONSchema            `json:"if,omitempty"`
	Then                 *JSONSchema            `json:"then,omitempty"`
	Else                 *JSONSchema            `json:"else,omitempty"`
	DependentSchemas     map[string]*JSONSchema `json:"dependentSchemas,omitempty"`
	DependentRequired    map[string][]string    `json:"dependentRequired,omitempty"`
	Contains             *JSONSchema            `json:"contains,omitempty"`
	PropertyNames        *JSONSchema            `json:"propertyNames,omitempty"`
	Anchor               string                 `json:"$anchor,omitempty"`
	Merge                bool                   `json:"$merge,omitempty"`
	RecursiveRef         bool                   `json:"$recursiveRef,omitempty"`
	RecursiveAnchor      bool                   `json:"$recursiveAnchor,omitempty"`
	DynamicRef           *JSONSchema            `json:"$dynamicRef,omitempty"`
	Vocabulary           map[string]*JSONSchema `json:"$vocabulary,omitempty"`
	Fluent               bool                   `json:"$fluent,omitempty"`
	Comment              string                 `json:"$comment,omitempty"`
	RefScope             string                 `json:"$refScope,omitempty"`
	Extension            map[string]interface{} `json:"-"`
}

type AvroSchema interface{}

type PrimitiveType string

const (
	NullType    PrimitiveType = "null"
	BooleanType PrimitiveType = "boolean"
	IntType     PrimitiveType = "int"
	LongType    PrimitiveType = "long"
	FloatType   PrimitiveType = "float"
	DoubleType  PrimitiveType = "double"
	BytesType   PrimitiveType = "bytes"
	StringType  PrimitiveType = "string"
)

type RecordField struct {
	Name         string      `json:"name"`
	Type         AvroSchema  `json:"type"`
	DefaultValue interface{} `json:"default,omitempty"`
	Doc          string      `json:"doc,omitempty"`
}

type RecordType struct {
	Type      string        `json:"type"`
	Name      string        `json:"name"`
	Namespace string        `json:"namespace,omitempty"`
	Aliases   []string      `json:"aliases,omitempty"`
	Doc       string        `json:"doc,omitempty"`
	Fields    []RecordField `json:"fields"`
}

type EnumSymbol string

type EnumType struct {
	Type    string       `json:"type"`
	Name    string       `json:"name"`
	Doc     string       `json:"doc,omitempty"`
	Symbols []EnumSymbol `json:"symbols"`
}

type ArrayType struct {
	Type  string     `json:"type"`
	Items AvroSchema `json:"items"`
}

type MapType struct {
	Type  string     `json:"type"`
	Items AvroSchema `json:"values"`
}

type UnionType struct {
	Types []AvroSchema `json:"type"`
}

type FixedType struct {
	Type      string `json:"type"`
	Name      string `json:"name"`
	Namespace string `json:"namespace,omitempty"`
	Size      int    `json:"size"`
}

type NameType struct {
	Type      string `json:"type"`
	Name      string `json:"name"`
	Namespace string `json:"namespace,omitempty"`
}

type AliasesType struct {
	Type    string   `json:"type"`
	Name    string   `json:"name"`
	Aliases []string `json:"aliases"`
}

type AnyType interface{}

func main() {
	schemaJSON := `
	{
		"$schema": "https://json-schema.org/draft/2020-12/schema",
		"$id": "https://example.com/product.schema.json",
		"title": "Product",
		"description": "A product from Acme's catalog",
		"type": "object",
		"properties": {
		  "productId": {
			"description": "The unique identifier for a product",
			"type": "number"
		  },
		  "productName": {
			"description": "Name of the product",
			"type": "string"
		  },
		  "price": {
			"description": "The price of the product",
			"type": "number",
			"exclusiveMinimum": 0
		  },
		  "tags": {
			"description": "Tags for the product",
			"type": "array",
			"items": {
			  "type": "string"
			},
			"minItems": 1,
			"uniqueItems": true
		  },
		  "dimensions": {
			"type": "object",
			"properties": {
			  "length": {
				"type": "number",
				"minimum": 100,
				"maximum": 1000
			  },
			  "width": {
				"type": "number",
				"minimum": 10,
				"maximum": 100
			  },
			  "height": {
				"type": "number",
				"minimum": 1,
				"maximum": 10
			  }
			},
			"required": [ "length", "width", "height" ]
		  }
		},
		"required": [ "productId",  "price" ]
	  }
	  `
	var schema JSONSchema
	err := json.Unmarshal([]byte(schemaJSON), &schema)
	if err != nil {
		fmt.Println("Error unmarshaling JSON schema:", err)
		return
	}

	// fmt.Printf("%+v\n", schema)

	res, err := jsonSchemaToAvroSchema([]byte(schemaJSON))
	if err != nil {
		fmt.Println("Error jsonSchemaToAvroSchema:", err)
		return
	}
	// fmt.Println(res)

	// Convert the result to a JSON string
	resJSON, err := json.Marshal(res)
	if err != nil {
		fmt.Println("Error marshaling to JSON:", err)
		return
	}

	// Print the JSON string
	fmt.Println(string(resJSON))

}

func walkSchema(schema *JSONSchema, recordName string) AvroSchema {
	switch schema.Type {
	case "null":
		return NullType
	case "boolean":
		return BooleanType
	case "integer":
		if schema.Format == "int64" {
			return LongType
		} else {
			return IntType
		}
	case "number":
		if schema.Format == "double" {
			return DoubleType
		} else {
			return FloatType
		}
	case "string":
		return StringType
	case "array":
		return walkArraySchema(schema)
	case "object":
		return walkObjectSchema(schema, recordName)
	default:
		// unknown type
		return nil
	}
}

func walkArraySchema(schema *JSONSchema) AvroSchema {
	if schema.Items != nil {
		return &ArrayType{
			Type:  "array",
			Items: walkSchema(schema.Items, ""),
		}
	}
	// If Items is not set, return an array of null and any type
	var any AnyType
	return &UnionType{
		Types: []AvroSchema{
			NullType,
			&any,
		},
	}
}

func walkObjectSchema(schema *JSONSchema, recordName string) AvroSchema {
	fields := make([]RecordField, 0, len(schema.Properties))
	for name, prop := range schema.Properties {
		isRequired := false
		for _, requiredProp := range schema.Required {
			if requiredProp == name {
				isRequired = true
				break
			}
		}

		// Check if additional properties are allowed
		if schema.AdditionalProperties != nil &&
			schema.AdditionalProperties.Type == "boolean" &&
			schema.AdditionalProperties.Enum != nil &&
			schema.AdditionalProperties.Enum[0] == false {
			if !isRequired {
				continue
			}
		}

		var fieldType AvroSchema

		if isRequired {
			fieldType = walkSchema(prop, name)
		} else {
			fieldType = &UnionType{
				Types: []AvroSchema{
					walkSchema(prop, name),
					NullType,
				},
			}
		}

		field := RecordField{
			Name: name,
			Type: fieldType,
		}
		if prop.Default != nil {
			field.DefaultValue = prop.Default
		}
		if prop.Description != "" {
			field.Doc = prop.Description
		}
		fields = append(fields, field)
	}
	if schema.Title == "" {
		schema.Title = fmt.Sprintf("%srecord", recordName)
	}
	return &RecordType{
		Type:   "record",
		Name:   schema.Title,
		Fields: fields,
	}
}

func jsonSchemaToAvroSchema(schemaJSON []byte) (AvroSchema, error) {
	schema := &JSONSchema{}
	err := json.Unmarshal(schemaJSON, schema)
	if err != nil {
		return nil, err
	}

	return walkSchema(schema, ""), nil
}
