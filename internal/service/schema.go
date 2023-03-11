package service

import (
	"context"
	"time"
)

type Schema struct {
	ID        primitive.ObjectID `bson:"_id,omitempty"`
	Name      string             `bson:"name"`
	Version   int                `bson:"version"`
	Schema    string             `bson:"schema"`
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

func (s *SchemaService) Create(schema *Schema) (*Schema, error) {
	schema.CreatedAt = time.Now()
	schema.UpdatedAt = time.Now()
	schema.Version = 1
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

func (s *SchemaService) Update(schema *Schema) error {
	schema.UpdatedAt = time.Now()
	filter := bson.M{"_id": schema.ID}
	update := bson.M{"$set": bson.M{
		"name":       schema.Name,
		"version":    schema.Version,
		"schema":     schema.Schema,
		"updated_at": schema.UpdatedAt,
	}}
	_, err := s.collection.UpdateOne(context.Background(), filter, update)
	return err
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
