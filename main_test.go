package main

import (
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type table struct {
	Pkey string `dynamodbav:"pkey"`
	Skey string `dynamodbav:"skey"`
}

func Test(t *testing.T) {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	dynamo := dynamodb.New(sess, &aws.Config{Region: aws.String("us-west-2")})

	tableName, ok := os.LookupEnv("TABLE_NAME")
	if !ok {
		t.Fatalf("Missing TABLE_NAME environment variable required test.")
	}

	// Setup

	updates := make([]table, 100)
	for i := range updates {
		updates[i] = table{
			Pkey: fmt.Sprintf("%v", math.Mod(float64(i), 10)),
			Skey: fmt.Sprintf("%v", i),
		}
	}

	// Item
	var itemResults []*dynamodb.ConsumedCapacity
	for _, item := range updates {
		res := itemTest(t, dynamo, tableName, item)
		itemResults = append(itemResults, res)
	}
	measure(t, "PutItem", itemResults)

	// Item + conditional
	var condtionalItemResults []*dynamodb.ConsumedCapacity
	for _, item := range updates {
		res := itemTestConditional(t, dynamo, tableName, item)
		condtionalItemResults = append(condtionalItemResults, res)
	}
	measure(t, "PutItem + Condition", condtionalItemResults)

	// Batch
	var batchResults []*dynamodb.ConsumedCapacity
	for _, table25 := range group25(updates) {
		res := batchTest(t, dynamo, tableName, table25)
		batchResults = append(batchResults, res...)
	}
	measure(t, "BatchWriteItem", batchResults)
}

func measure(t *testing.T, name string, results []*dynamodb.ConsumedCapacity) {
	capacityUnits := 0.0
	readCapacityUnits := 0.0
	writeCapacityUnits := 0.0

	for _, res := range results {
		if res != nil {
			capacity := *res
			if capacity.CapacityUnits != nil {
				capacityUnits += *capacity.CapacityUnits
			}
			if capacity.WriteCapacityUnits != nil {
				writeCapacityUnits += *capacity.WriteCapacityUnits
			}

			if capacity.ReadCapacityUnits != nil {
				readCapacityUnits += *capacity.ReadCapacityUnits
			}
		}
	}

	t.Logf("%s consumed capacity:\n", name)
	t.Logf(" - %v capacity units\n", capacityUnits)
	t.Logf(" - %v write capacity units\n", writeCapacityUnits)
	t.Logf(" - %v read capacity units\n", readCapacityUnits)

}

func itemTest(t *testing.T, dynamo *dynamodb.DynamoDB, tableName string, update table) *dynamodb.ConsumedCapacity {
	var err error
	av, err := dynamodbattribute.MarshalMap(update)
	if err != nil {
		t.Fatalf("Unable to marshal map - %s\n", err.Error())
	}

	req := &dynamodb.PutItemInput{
		TableName:              aws.String(tableName),
		Item:                   av,
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}

	res, err := dynamo.PutItem(req)

	if err != nil {
		t.Fatal(err)
	}

	return res.ConsumedCapacity
}

func itemTestConditional(t *testing.T, dynamo *dynamodb.DynamoDB, tableName string, update table) *dynamodb.ConsumedCapacity {
	var err error
	av, err := dynamodbattribute.MarshalMap(update)
	if err != nil {
		t.Fatalf("Unable to marshal map - %s\n", err.Error())
	}

	req := &dynamodb.PutItemInput{
		TableName:              aws.String(tableName),
		ConditionExpression:    aws.String("attribute_not_exists(other_column)"),
		Item:                   av,
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}

	res, err := dynamo.PutItem(req)

	if err != nil {
		t.Fatal(err)
	}

	return res.ConsumedCapacity
}

func batchTest(t *testing.T, dynamo *dynamodb.DynamoDB, tableName string, updates []table) []*dynamodb.ConsumedCapacity {
	var err error
	requestItems := make(map[string][]*dynamodb.WriteRequest)

	for i := range updates {
		var putReq dynamodb.PutRequest
		putReq.Item, err = dynamodbattribute.MarshalMap(updates[i])
		if err != nil {
			t.Fatalf("Unable to marshal batch - %s\n", err.Error())
		}
		requestItems[tableName] = append(requestItems[tableName], &dynamodb.WriteRequest{PutRequest: &putReq})
	}

	res, err := dynamo.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems:           requestItems,
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	})

	if err != nil {
		t.Fatal(err)
	}

	return res.ConsumedCapacity

}

func group25(in []table) [][]table {
	n := 25

	var j int
	var out []table
	outs := make([][]table, 0)

	for i, e := range in {
		j = i % n
		if j == 0 {
			out = make([]table, n)
			outs = append(outs, out)
		}
		out[j] = e
	}

	// trim last 'out' to length
	out = out[:j+1]
	// update last 'out'
	outs[len(outs)-1] = out

	return outs
}
