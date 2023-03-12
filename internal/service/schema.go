package service

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Schema struct {
	ID        primitive.ObjectID `bson:"_id,omitempty"`
	Name      string             `bson:"name"`
	Version   int                `bson:"version"`
	Schema    bson.M             `bson:"schema"`
	CreatedAt time.Time          `bson:"created_at"`
	UpdatedAt time.Time          `bson:"updated_at"`
}

type SchemaService struct {
	collection *mongo.Collection
}

func NewSchemaService(client *mongo.Client, dbName, collectionName string) *SchemaService {
	collection := client.Database(dbName).Collection(collectionName)
	return &SchemaService{collection}
}

func (s *SchemaService) Create(schema *Schema, schemaBytes []byte) (*Schema, error) {

	// Check if a schema with the same name and version already exists
	var existingSchema Schema
	err := s.collection.FindOne(
		context.Background(),
		bson.M{"name": schema.Name},
	).Decode(&existingSchema)
	if err == nil {
		return nil, fmt.Errorf("schema with name %s already exists", schema.Name)
	} else if err != mongo.ErrNoDocuments {
		return nil, err
	}

	schema.CreatedAt = time.Now()
	schema.UpdatedAt = time.Now()
	schema.Version = 1

	// Convert incoming JSON string to bson.M document
	var schemaDoc bson.M
	err = bson.UnmarshalExtJSON(schemaBytes, true, &schemaDoc)
	if err != nil {
		return nil, err
	}
	schema.Schema = schemaDoc

	res, err := s.collection.InsertOne(context.Background(), schema)
	if err != nil {
		return nil, err
	}
	schema.ID = res.InsertedID.(primitive.ObjectID)

	return schema, nil
}

func (s *SchemaService) FindAll() ([]*Schema, error) {
	cursor, err := s.collection.Find(context.Background(), bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())

	schemas := []*Schema{}
	for cursor.Next(context.Background()) {
		schema := &Schema{}
		err := cursor.Decode(schema)
		if err != nil {
			return nil, err
		}
		schemas = append(schemas, schema)
	}
	return schemas, nil
}

func (s *SchemaService) FindByID(id string) (*Schema, error) {
	objID, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, err
	}
	schema := &Schema{}
	err = s.collection.FindOne(context.Background(), bson.M{"_id": objID}).Decode(schema)
	if err != nil {
		return nil, err
	}
	return schema, nil
}

func (s *SchemaService) FindByName(name string) (*Schema, error) {
	opts := options.FindOne().SetSort(bson.M{"version": -1})
	schema := &Schema{}
	err := s.collection.FindOne(context.Background(), bson.M{"name": name}, opts).Decode(schema)
	if err != nil {
		return nil, err
	}
	return schema, nil
}

func (s *SchemaService) FindByNameAndVersion(name string, version int) (*Schema, error) {
	query := bson.M{
		"name":    name,
		"version": version,
	}
	schema := &Schema{}
	err := s.collection.FindOne(context.Background(), query).Decode(schema)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, fmt.Errorf("schema with name '%s' and version '%d' not found", name, version)
		}
		return nil, err
	}
	return schema, nil
}

func (s *SchemaService) Update(schema *Schema) (*Schema, error) {
	// We are not actually updating, we insert the schema with a higher version number
	// Unset the ID to force insertion of a new document
	schema.ID = primitive.NilObjectID
	schema.UpdatedAt = time.Now()
	schema.Version++
	res, err := s.collection.InsertOne(context.Background(), schema)
	if err != nil {
		return nil, err
	}
	schema.ID = res.InsertedID.(primitive.ObjectID)
	return schema, nil
}

func (s *SchemaService) Delete(id string) error {
	objID, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return err
	}
	filter := bson.M{"_id": objID}
	_, err = s.collection.DeleteOne(context.Background(), filter)
	return err
}
