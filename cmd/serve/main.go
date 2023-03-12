package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"strconv"

	"github.com/actgardner/gogen-avro/v10/compiler"

	"github.com/labstack/echo/v4"
	"github.com/xeipuuv/gojsonschema"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/tradeface/schema-registry/internal/service"
)

type App struct {
	Router        *echo.Echo
	DB            *mongo.Client
	schemaService *service.SchemaService
}

func main() {
	// connect to mongodb
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://root:example@localhost:27017"))
	if err != nil {
		log.Fatalf("Failed to create mongo client: %v", err)
	}
	err = client.Connect(context.Background())
	if err != nil {
		log.Fatalf("Failed to connect to mongo: %v", err)
	}
	// create schema service
	schemaService := service.NewSchemaService(client, "schema_registry", "schemas")

	// create app
	app := &App{
		Router:        echo.New(),
		DB:            client,
		schemaService: schemaService,
	}

	// register routes
	app.Router.GET("/schemas", app.handleGetSchemas)
	app.Router.POST("/schemas/:name", app.handleCreateSchema)
	app.Router.GET("/schemas/:name", app.handleGetSchema)
	app.Router.PUT("/schemas/:name", app.handleUpdateSchema)
	app.Router.GET("/schemas/:name/avro", app.handleGetAvroSchema)
	app.Router.GET("/schemas/:name/:version", app.handleGetSchemaWithVersion)

	// start server
	err = app.Router.Start(":8082")
	if err != nil && err != http.ErrServerClosed {
		log.Fatalf("Failed to start server: %v", err)
	}
}

func NewApp(schemaService *service.SchemaService) *App {
	app := &App{
		Router:        echo.New(),
		schemaService: schemaService,
	}
	return app
}

func (a *App) Start(addr string) error {
	a.Router.GET("/schemas", a.handleGetSchemas)
	a.Router.POST("/schemas/:name", a.handleCreateSchema)
	a.Router.GET("/schemas/:name", a.handleGetSchema)
	a.Router.GET("/schemas/:name/avro", a.handleGetAvroSchema)
	a.Router.GET("/schemas/:name/:version", a.handleGetSchemaWithVersion)
	a.Router.PUT("/schemas/:name", a.handleUpdateSchema)
	// a.Router.DELETE("/schemas/:id", a.handleDeleteSchema)

	return a.Router.Start(addr)
}

func (a *App) handleGetSchemas(c echo.Context) error {
	schemas, err := a.schemaService.FindAll()
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, schemas)
}

func (a *App) handleCreateSchema(c echo.Context) error {
	schema := &service.Schema{}
	requestBody, err := ioutil.ReadAll(c.Request().Body)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}

	var schemaDoc bson.M
	err = bson.UnmarshalExtJSON(requestBody, true, &schemaDoc)
	if err != nil {
		return err
	}
	schema.Name = c.Param("name")
	schema.Schema = schemaDoc

	if err := validateSchema(string(requestBody)); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}
	result, err := a.schemaService.Create(schema, requestBody)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusCreated, result)
}

func (a *App) handleGetAvroSchema(c echo.Context) error {
	schemaName := c.Param("name")
	schema, err := a.schemaService.FindByName(schemaName)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return c.JSON(http.StatusNotFound, map[string]string{"error": "schema not found"})
		}
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	// Convert BSON schema to JSON schema
	jsonSchema, err := bson.MarshalExtJSON(schema.Schema, false, false)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	// Compile JSON schema to Avro schema
	cType, err := compiler.ParseSchema(jsonSchema)
	if err == nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to compile schema"})
	}
	fmt.Println(string(jsonSchema))
	fmt.Println(cType)

	// Get the compiled Avro schema as JSON
	// schemaJSON, err := cType.MarshalJSON()
	// if err != nil {
	// 	return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	// }
	schemaJSON := []byte{97, 98, 99, 100, 101, 102}

	return c.Blob(http.StatusOK, "application/json", schemaJSON)
}

// func (a *App) handleGetAvroScxhema(c echo.Context) error {
// 	schema, err := a.schemaService.FindByName(c.Param("name"))
// 	if err != nil {
// 		if err == mongo.ErrNoDocuments {
// 			return c.JSON(http.StatusNotFound, map[string]string{"error": "schema not found"})
// 		}
// 		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
// 	}

// 	// Convert BSON schema to Avro schema
// 	var bsonSchema map[string]interface{}
// 	err = bson.Unmarshal([]byte(schema), &bsonSchema)
// 	if err != nil {
// 		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
// 	}

// 	avroSchema, err := compiler.CompileSchema(bsonSchema)
// 	if err != nil {
// 		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
// 	}

// 	// Convert Avro schema to JSON
// 	var avroSchemaJSON bytes.Buffer
// 	err = compiler.WriteSchema(&avroSchemaJSON, avroSchema)
// 	if err != nil {
// 		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
// 	}

// 	var avroSchemaMap map[string]interface{}
// 	err = json.Unmarshal(avroSchemaJSON.Bytes(), &avroSchemaMap)
// 	if err != nil {
// 		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
// 	}

// 	return c.JSON(http.StatusOK, avroSchemaMap)
// }

func (a *App) handleGetSchema(c echo.Context) error {
	schema, err := a.schemaService.FindByName(c.Param("name"))
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return c.JSON(http.StatusNotFound, map[string]string{"error": "schema not found"})
		}
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, schema)
}

func (a *App) handleGetSchemaWithVersion(c echo.Context) error {

	version, err := strconv.Atoi(c.Param("version"))
	if err != nil {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "schema not found"})
	}
	schema, err := a.schemaService.FindByNameAndVersion(c.Param("name"), version)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return c.JSON(http.StatusNotFound, map[string]string{"error": "schema not found"})
		}
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, schema)
}

// func (a *App) handleUpdateSchcema(c echo.Context) error {
// 	schema, err := a.schemaService.FindByName(c.Param("name"))
// 	if err != nil {
// 		if err == mongo.ErrNoDocuments {
// 			return c.JSON(http.StatusNotFound, map[string]string{"error": "schema not found"})
// 		}
// 		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
// 	}

// 	// Store the existing schema in a temporary object
// 	tempSchema := &service.Schema{
// 		ID:        schema.ID,
// 		Name:      schema.Name,
// 		Version:   schema.Version,
// 		Schema:    schema.Schema,
// 		CreatedAt: schema.CreatedAt,
// 		UpdatedAt: schema.UpdatedAt,
// 	}

// 	// if err := c.Bind(schema); err != nil {
// 	// 	return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
// 	// }

// 	// Read the request body into a byte slice
// 	requestBody, err := ioutil.ReadAll(c.Request().Body)
// 	if err != nil {
// 		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
// 	}
// 	schema.Name = c.Param("name")
// 	schema.Schema = string(requestBody)

// 	// Clean up the schema string from spaces
// 	schema.Schema = strings.ReplaceAll(schema.Schema, " ", "")

// 	// Compare the incoming schema with the existing schema in the database
// 	if schema.Schema == strings.ReplaceAll(tempSchema.Schema, " ", "") {
// 		return c.JSON(http.StatusConflict, map[string]string{"error": "schema already exists"})
// 	}

// 	if err := validateSchema(string(requestBody)); err != nil {
// 		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
// 	}
// 	schema, err = a.schemaService.Update(schema)
// 	if err != nil {
// 		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
// 	}
// 	return c.JSON(http.StatusOK, schema)
// }

func (a *App) handleUpdateSchema(c echo.Context) error {

	schema, err := a.schemaService.FindByName(c.Param("name"))
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return c.JSON(http.StatusNotFound, map[string]string{"error": "schema not found"})
		}
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	requestBody, err := ioutil.ReadAll(c.Request().Body)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}

	var schemaDoc bson.M
	err = bson.UnmarshalExtJSON(requestBody, true, &schemaDoc)
	if err != nil {
		return err
	}
	schema.Schema = schemaDoc

	// Validate the incoming schema
	if err := validateSchema(string(requestBody)); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}

	// Check if incoming schema is equal to existing schema
	existingSchema, err := a.schemaService.FindByName(schema.Name)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			// Schema does not exist, continue with update
		} else {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
	} else {
		// Compare the incoming schema with the existing schema in the database
		if reflect.DeepEqual(existingSchema.Schema, schema.Schema) {
			return c.JSON(http.StatusConflict, map[string]string{"error": "schema already exists"})
		}
	}

	schema, err = a.schemaService.Update(schema)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, schema)
}

// func (a *App) handleDeleteSchema(c echo.Context) error {
// 	if err := a.schemaService.Delete(c.Param("id")); err != nil {
// 		if err == mongo.ErrNoDocuments {
// 			return c.JSON(http.StatusNotFound, map[string]string{"error": "schema not found"})
// 		}
// 		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
// 	}
// 	return c.NoContent(http.StatusNoContent)
// }

func validateSchema(schema string) error {
	loader := gojsonschema.NewStringLoader(schema)
	schemaDoc := gojsonschema.NewStringLoader(`{"$schema": "http://json-schema.org/draft-07/schema"}`)
	result, err := gojsonschema.Validate(schemaDoc, loader)
	if err != nil {
		return err
	}
	if result.Valid() {
		return nil
	}
	var errMsg string
	for _, err := range result.Errors() {
		errMsg += err.String() + "\n"
	}
	return fmt.Errorf("invalid schema: %s", errMsg)
}
