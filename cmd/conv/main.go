package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

type SchemaObject struct {
	Type          interface{}
	Enum          []interface{}
	Const         interface{}
	Id            string
	Schema        string
	Title         string
	Description   string
	Properties    map[string]interface{}
	Required      []string
	MaxProperties uint64
	MinProperties uint64
}

type SchemaArray struct {
	Type        interface{}
	Enum        []interface{}
	Const       interface{}
	Description string
	Items       *SchemaArrayItems
	MinItems    uint64
	MaxItems    uint64
	MaxContains uint64
	MinContains uint64
	UniqueItems bool
}

type SchemaArrayItems struct {
	Type string
}

type SchemaNumber struct {
	Type             interface{}
	Enum             []interface{}
	Const            interface{}
	Description      string
	MultipleOf       float64
	Minimum          float64
	ExclusiveMinimum float64
	Maximum          float64
	ExclusiveMaximum float64
}

type SchemaString struct {
	Type        interface{}
	Enum        []interface{}
	Const       interface{}
	Description string
	Pattern     string
	MaxLength   uint64
	MinLength   uint64
}

func main() {
	schemaStr := `
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
		"required": [ "productId", "productName", "price" ]
	  }
	  `

	schemaMap := make(map[string]interface{})
	if err := json.Unmarshal([]byte(schemaStr), &schemaMap); err != nil {
		panic(err)
	}
	// TODO: use populate functions
	schemaObject := SchemaObject{
		Id:          schemaMap["$id"].(string),
		Schema:      schemaMap["$schema"].(string),
		Description: schemaMap["description"].(string),
		Title:       schemaMap["title"].(string),
		Properties:  schemaMap["properties"].(map[string]interface{}),
		Required:    toStringSlice(schemaMap["required"].([]interface{})),
	}
	walkProperties(schemaObject.Properties, &schemaObject)

	fmt.Println(schemaObject)
	fmt.Println(displaySchema(&schemaObject, 0))

	avrox, err := buildAvroSchema(&schemaObject)
	if err != nil {
		return
	}
	data, err := json.Marshal(avrox)
	if err != nil {
		return
	}
	fmt.Println(string(data))
}

func displaySchema(schema *SchemaObject, indent int) string {
	var b strings.Builder

	// write the current schema object's title and description
	b.WriteString(strings.Repeat(" ", indent))
	b.WriteString(schema.Title)
	if schema.Description != "" {
		b.WriteString(fmt.Sprintf(" (%s)", schema.Description))
	}
	b.WriteString("\n")

	// write out the properties
	for propName, propSchema := range schema.Properties {
		// write the property name
		b.WriteString(strings.Repeat(" ", indent+2))
		b.WriteString(propName)
		b.WriteString(": ")

		// write the property schema
		switch propSchema := propSchema.(type) {
		case *SchemaObject:
			b.WriteString("\n")
			b.WriteString(displaySchema(propSchema, indent+4))
		case *SchemaArray:
			b.WriteString(fmt.Sprintf("%sItems: %s\n", indent, propSchema.Items.Type))
		case *SchemaNumber:
			b.WriteString(fmt.Sprintf("Number (%v to %v)\n", propSchema.Minimum, propSchema.Maximum))
		case *SchemaString:
			b.WriteString(fmt.Sprintf("String (%d to %d characters)\n", propSchema.MinLength, propSchema.MaxLength))
		}
	}

	return b.String()
}

func toStringSlice(input []interface{}) []string {
	result := make([]string, len(input))
	for i, v := range input {
		result[i] = v.(string)
	}
	return result
}

func getSchemaType(obj map[string]interface{}) interface{} {
	if val, ok := obj["type"]; ok {
		switch val := val.(type) {
		case string:
			return val
		case []interface{}:
			types := make([]string, len(val))
			for i, t := range val {
				types[i] = t.(string)
			}
			return types
		}
	}
	return nil
}

func getSchemaEnum(obj map[string]interface{}) []interface{} {
	if val, ok := obj["enum"]; ok {
		if enums, ok := val.([]interface{}); ok {
			result := make([]interface{}, len(enums))
			for i, e := range enums {
				result[i] = e
			}
			return result
		}
	}

	return nil
}

func getSchemaConst(obj map[string]interface{}) interface{} {
	if val, ok := obj["const"]; ok {
		return val
	}
	return nil
}

func getSchemaDescription(obj map[string]interface{}) interface{} {
	if val, ok := obj["description"]; ok {
		return val.(string)
	}
	return nil
}

func populateGeneral(obj map[string]interface{}, schema interface{}) {

	if t := getSchemaType(obj); t != nil {
		reflect.ValueOf(schema).Elem().FieldByName("Type").Set(reflect.ValueOf(t))
	}
	if enums := getSchemaEnum(obj); enums != nil {
		reflect.ValueOf(schema).Elem().FieldByName("Enum").Set(reflect.ValueOf(enums))
	}
	if c := getSchemaConst(obj); c != nil {
		reflect.ValueOf(schema).Elem().FieldByName("Const").Set(reflect.ValueOf(c))
	}
	if d := getSchemaDescription(obj); d != nil {
		reflect.ValueOf(schema).Elem().FieldByName("Description").Set(reflect.ValueOf(d))
	}
}

func populateSchemaObject(obj map[string]interface{}, schema *SchemaObject) {

	populateGeneral(obj, schema)

	if val, ok := obj["$id"]; ok {
		schema.Id = val.(string)
	}
	if val, ok := obj["$schema"]; ok {
		schema.Schema = val.(string)
	}
	if val, ok := obj["title"]; ok {
		schema.Title = val.(string)
	}
	if val, ok := obj["properties"]; ok {
		schema.Properties = val.(map[string]interface{})
	}
	if val, ok := obj["required"]; ok {
		schema.Required = toStringSlice(val.([]interface{}))
	}
	if val, ok := obj["maxProperties"]; ok {
		schema.MaxProperties = uint64(val.(float64))
	}

	if val, ok := obj["minProperties"]; ok {
		schema.MinProperties = uint64(val.(float64))
	}
}

func populateSchemaArrayItems(obj map[string]interface{}, schema *SchemaArrayItems) {
	if items, ok := obj["items"].(map[string]interface{}); ok {
		if itemType, ok := items["type"].(string); ok {
			schema.Type = itemType
		}
	}
}

func populateSchemaArray(obj map[string]interface{}, schema *SchemaArray) {

	populateGeneral(obj, schema)
	schemaItems := &SchemaArrayItems{}
	populateSchemaArrayItems(obj, schemaItems)
	schema.Items = schemaItems

	if v, ok := obj["minItems"]; ok {
		schema.MinItems = uint64(v.(float64))
	}
	if v, ok := obj["maxItems"]; ok {
		schema.MaxItems = uint64(v.(float64))
	}
	if v, ok := obj["maxContains"]; ok {
		schema.MaxContains = uint64(v.(float64))
	}
	if v, ok := obj["minContains"]; ok {
		schema.MinContains = uint64(v.(float64))
	}
	if v, ok := obj["uniqueItems"]; ok {
		schema.UniqueItems = v.(bool)
	}
}

func populateSchemaNumber(obj map[string]interface{}, schema *SchemaNumber) {

	populateGeneral(obj, schema)

	if v, ok := obj["multipleOf"].(float64); ok {
		schema.MultipleOf = v
	}
	if v, ok := obj["minimum"].(float64); ok {
		schema.Minimum = v
	}
	if v, ok := obj["exclusiveMinimum"].(float64); ok {
		schema.ExclusiveMinimum = v
	}
	if v, ok := obj["maximum"].(float64); ok {
		schema.Maximum = v
	}
	if v, ok := obj["exclusiveMaximum"].(float64); ok {
		schema.ExclusiveMaximum = v
	}
}

func populateSchemaString(obj map[string]interface{}, schema *SchemaString) {

	populateGeneral(obj, schema)

	if val, ok := obj["pattern"]; ok {
		schema.Pattern = val.(string)
	}
	if val, ok := obj["maxLength"]; ok {
		schema.MaxLength = uint64(val.(float64))
	}
	if val, ok := obj["minLength"]; ok {
		schema.MinLength = uint64(val.(float64))
	}
}

func walkProperties(properties map[string]interface{}, schema *SchemaObject) {
	for name, property := range properties {
		propMap := property.(map[string]interface{})
		propType := propMap["type"].(string)

		fullName := name
		switch propType {
		case "object":
			propertySchema := &SchemaObject{}
			populateSchemaObject(propMap, propertySchema)
			schema.Properties[name] = propertySchema
			walkProperties(propertySchema.Properties, propertySchema)
		case "array":
			propertySchema := &SchemaArray{}
			populateSchemaArray(propMap, propertySchema)
			schema.Properties[name] = propertySchema
		case "number":
			propertySchema := &SchemaNumber{}
			populateSchemaNumber(propMap, propertySchema)
			schema.Properties[name] = propertySchema
		case "string":
			propertySchema := &SchemaString{}
			populateSchemaString(propMap, propertySchema)
			schema.Properties[name] = propertySchema
		default:
			fmt.Printf("unknown %s: %s\n", fullName, propType)
		}
	}
}

///avro
// TODO: the function should accept any start point
// TODO: check which properties for json-schema should be added
// TODO: implement Union "null"
// TODO: what about defaults?
// TODO: split up function it is too big

func buildAvroSchema(obj *SchemaObject) (interface{}, error) {
	schema := make(map[string]interface{})
	schema["type"] = "record"
	schema["name"] = obj.Title
	schema["namespace"] = obj.Title

	if obj.Description != "" {
		schema["doc"] = obj.Description
	}

	fields := make([]map[string]interface{}, 0)

	for key, value := range obj.Properties {
		field := make(map[string]interface{})
		field["name"] = key

		switch v := value.(type) {
		case *SchemaObject:
			v.Title = fmt.Sprintf("%srecord", key)
			fieldType, err := buildAvroSchema(v)
			if err != nil {
				return "", err
			}
			field["type"] = fieldType
			if v.Description != "" {
				field["doc"] = v.Description
			}
		case *SchemaArray:
			field["type"] = map[string]interface{}{
				"type":  "array",
				"items": v.Items.Type,
			}
			if v.Description != "" {
				field["doc"] = v.Description
			}
		case *SchemaNumber:
			switch v.Type {
			case "integer":
				field["type"] = "int"
			case "number":
				field["type"] = "float"
			}
			if v.Description != "" {
				field["doc"] = v.Description
			}
		case *SchemaString:
			field["type"] = "string"
			if v.Description != "" {
				field["doc"] = v.Description
			}
		default:
			return "", fmt.Errorf("unknown schema type: %T", v)
		}
		fields = append(fields, field)
	}

	schema["fields"] = fields

	return schema, nil
}
