package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"go.mongodb.org/mongo-driver/mongo"
)

type App struct {
	Router *echo.Echo
	DB     *mongo.Database
}

func main() {
	app := &App{}

	// initialize the router
	app.Router = echo.New()

	// add middleware
	app.Router.Use(middleware.Logger())
	app.Router.Use(middleware.Recover())

	// connect to mongodb
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatalf("Failed to create mongo client: %v", err)
	}
	err = client.Connect(context.Background())
	if err != nil {
		log.Fatalf("Failed to connect to mongo: %v", err)
	}
	app.DB = client.Database("schema_registry")

	// register routes
	app.Router.GET("/schemas/:id", app.getSchema)
	app.Router.GET("/schemas/:id/avro", app.getAvro)
	app.Router.POST("/schemas", app.createSchema)
	app.Router.PUT("/schemas/:id", app.updateSchema)
	app.Router.DELETE("/schemas/:id", app.deleteSchema)

	// start server
	err = app.Router.Start(":8080")
	if err != nil && err != http.ErrServerClosed {
		log.Fatalf("Failed to start server: %v", err)
	}
}

// type App struct {
// 	*echo.Echo
// 	schemaService *service.SchemaService
// }

func NewApp(schemaService *service.SchemaService) *App {
	app := echo.New()
	return &App{app, schemaService}
}

func (a *App) Start(addr string) error {
	a.GET("/schemas", a.handleGetSchemas)
	a.POST("/schemas", a.handleCreateSchema)
	a.GET("/schemas/:id", a.handleGetSchema)
	a.PUT("/schemas/:id", a.handleUpdateSchema)
	a.DELETE("/schemas/:id", a.handleDeleteSchema)

	return a.Echo.Start(addr)
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
	if err := c.Bind(schema); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}
	if err := validateSchema(schema.Schema); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}
	result, err := a.schemaService.Create(schema)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusCreated, result)
}

func (a *App) handleGetSchema(c echo.Context) error {
	schema, err := a.schemaService.FindByID(c.Param("id"))
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return c.JSON(http.StatusNotFound, map[string]string{"error": "schema not found"})
		}
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, schema)
}

func (a *App) handleUpdateSchema(c echo.Context) error {
	schema, err := a.schemaService.FindByID(c.Param("id"))
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return c.JSON(http.StatusNotFound, map[string]string{"error": "schema not found"})
		}
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	if err := c.Bind(schema); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}
	if err := validateSchema(schema.Schema); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
	}
	if err := a.schemaService.Update(schema); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, schema)
}

func (a *App) handleDeleteSchema(c echo.Context) error {
	if err := a.schemaService.Delete(c.Param("id")); err != nil {
		if err == mongo.ErrNoDocuments {
			return c.JSON(http.StatusNotFound, map[string]string{"error": "schema not found"})
		}
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.NoContent(http.StatusNoContent)
}

func validateSchema(schema string) error {
	loader := jsonschema.NewStringLoader(schema)
	schemaDoc := jsonschema.Draft2019_09.Schema()
	validator, err := jsonschema.Validate(schemaDoc, loader)
	if err != nil {
		return err
	}
	if validator.Valid() {
		return nil
	}
	var errMsg string
	for _, err := range validator.Errors() {
		errMsg += err.String() + "\n"
	}
	return fmt.Errorf("invalid schema: %s", errMsg)
}
