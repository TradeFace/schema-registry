// package main

// import (
// 	"context"
// 	"encoding/json"
// 	"fmt"

// 	"github.com/apache/pulsar-client-go/pulsar"
// 	"github.com/linkedin/goavro/v2"
// )

// type Person struct {
// 	Name string `json:"name"`
// 	Age  int    `json:"age"`
// }

// func main() {
// 	client, err := pulsar.NewClient(pulsar.ClientOptions{
// 		URL: "pulsar://localhost:6650",
// 	})
// 	if err != nil {
// 		fmt.Println("Could not instantiate Pulsar client:", err)
// 		return
// 	}
// 	defer client.Close()

// 	producer, err := client.CreateProducer(pulsar.ProducerOptions{
// 		Topic: "persistent://public/default/my_input_topic",
// 	})
// 	if err != nil {
// 		fmt.Println("Could not create producer:", err)
// 		return
// 	}
// 	defer producer.Close()

// 	codec, err := goavro.NewCodec(`{"type":"record","name":"Person","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}`)
// 	if err != nil {
// 		fmt.Println("Could not create Avro codec:", err)
// 		return
// 	}

// 	person := Person{Name: "John", Age: 30}

// 	// Convert the Person record to a map[string]interface{}

// 	personBytes, err := json.Marshal(person)
// 	if err != nil {
// 		fmt.Println("Could not marshal Person to JSON:", err)
// 		return
// 	}

// 	// Convert the JSON byte array to a map[string]interface{}
// 	personMap := make(map[string]interface{})
// 	err = json.Unmarshal(personBytes, &personMap)
// 	if err != nil {
// 		fmt.Println("Could not unmarshal Person to map:", err)
// 		return
// 	}

// 	// Encode the map as an Avro binary record
// 	avroData, err := codec.BinaryFromNative(nil, personMap)
// 	if err != nil {
// 		fmt.Println("Failed to serialize message data to Avro:", err)
// 		return
// 	}

// 	// Send the message
// 	msg := &pulsar.ProducerMessage{
// 		Payload: avroData,
// 	}
// 	_, err = producer.Send(context.Background(), msg)
// 	if err != nil {
// 		fmt.Println("Failed to send message:", err)
// 		return
// 	}

// 	fmt.Println("Message sent successfully!")
// }
package main

func main() {

}
