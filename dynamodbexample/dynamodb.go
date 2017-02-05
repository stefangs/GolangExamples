package dynamodbexample

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"log"
	"os"
)

type Account struct {
	Name        string `json:"name"`
	Key         string `json:"key"`
	Description string `json:"description"`
}

func Main() {
	isLocalDatabase := len(os.Args) > 1 && os.Args[1] == "local"
	svc := openDatabase(isLocalDatabase)
	// If we are using a local DB, we create the table from this program, as there is no GUI for the local dynamoDB.
	// If we are using a real dynamoDB the table should be created via the console, since programs should not really
	// have rights to manipulate the database schema
	if isLocalDatabase && !contains(listTables(svc), "Accounts") {
		createTable(svc)
	}
	acc := &Account{Name: "Foo", Key: "123456", Description: "My first account"}
	insertAccount(svc, acc)
	findAccount(svc, "Foo")
	acc = &Account{Name: "Fum", Key: "654321", Description: "My seccond account"}
	insertAccount(svc, acc)
	listAccounts(svc)
	deleteAccount(svc, "Foo")
	deleteAccount(svc, "Fum")
	listAccounts(svc)
}

func openDatabase(localDB bool) *dynamodb.DynamoDB {
	config := &aws.Config{Region: aws.String("eu-central-1")}
	if localDB {
		// Here we are using a locally installed dynamoDB
		config.Endpoint = aws.String("http://127.0.0.1:8000")
	} else {
		// Here we are using a real dynamoDB at AWS. You need to create an IAM-account with rights to access
		// your "Account"-table and get the public and secret key for that account. We are using a shared
		// credentials file with the profile name: "home-cloud" where the keys are stored.
		// See https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html
		// For more details
		config.Credentials = credentials.NewSharedCredentials("", "home-cloud")
	}
	sess, err := session.NewSession(config)
	panicOnError(err)
	return dynamodb.New(sess)
}

func listTables(svc *dynamodb.DynamoDB) []*string {
	params := &dynamodb.ListTablesInput{
		Limit: aws.Int64(10),
	}
	resp, err := svc.ListTables(params)
	panicOnError(err)
	fmt.Println(resp)
	return resp.TableNames
}

func createTable(svc *dynamodb.DynamoDB) {
	params := &dynamodb.CreateTableInput{
		TableName: aws.String("Accounts"),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("AccountName"),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("AccountName"),
				KeyType:       aws.String(dynamodb.KeyTypeHash),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
	}
	resp, err := svc.CreateTable(params)
	panicOnError(err)
	fmt.Println(resp)
}

func insertAccount(svc *dynamodb.DynamoDB, srv *Account) {
	data, e := dynamodbattribute.Marshal(*srv)
	panicOnError(e)
	params := &dynamodb.PutItemInput{
		TableName: aws.String("Accounts"),
		Item: map[string]*dynamodb.AttributeValue{
			"AccountName": {S: aws.String(srv.Name)},
			"Data":        {M: map[string]*dynamodb.AttributeValue{"object": data}},
		},
	}
	_, err := svc.PutItem(params)
	panicOnError(err)
}

func findAccount(svc *dynamodb.DynamoDB, name string) *Account {
	params := &dynamodb.QueryInput{
		TableName:                 aws.String("Accounts"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{":nameValue": {S: aws.String(name)}},
		KeyConditionExpression:    aws.String("AccountName = :nameValue"),
		ConsistentRead:            aws.Bool(true),
		Limit:                     aws.Int64(1),
	}
	resp, err := svc.Query(params)
	panicOnError(err)
	if *resp.Count == 1 {
		acc := &Account{}
		e := dynamodbattribute.Unmarshal(resp.Items[0]["Data"].M["object"], acc)
		panicOnError(e)
		fmt.Printf("Find Account: %v\n", acc)
		return acc
	}
	return nil
}

func listAccounts(svc *dynamodb.DynamoDB) []*Account {
	params := &dynamodb.ScanInput{
		TableName: aws.String("Accounts"),
		ConsistentRead:      aws.Bool(true),
		Limit:                  aws.Int64(100),
	}
	resp, err := svc.Scan(params)
	panicOnError(err)
	accounts := make([]*Account, 0, int(*resp.Count))
	for _,row := range resp.Items {
		acc := &Account{}
		e := dynamodbattribute.Unmarshal(row["Data"].M["object"], acc)
		panicOnError(e)
		accounts = append(accounts, acc)
		fmt.Printf("List Account: %v\n", acc)
	}
	return accounts
}

func deleteAccount(svc *dynamodb.DynamoDB, name string)  {
	params := &dynamodb.DeleteItemInput{
		TableName: aws.String("Accounts"),
		Key: map[string]*dynamodb.AttributeValue{"AccountName": {S: aws.String(name)}},
	}
	_, err := svc.DeleteItem(params)
	panicOnError(err)
}

func panicOnError(e error) {
	if e != nil {
		log.Panic(e)
	}
}

func contains(list []*string, value string) bool {
	for _, s := range list {
		if *s == value {
			return true
		}
	}
	return false
}
