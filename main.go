package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/oracle/oci-go-sdk/common"
	"github.com/oracle/oci-go-sdk/nosql"
)

func main() {
	if len(os.Args) != 2 {
		panic("You have to specify compartment-id to command-line argument")
	}
	conf := common.DefaultConfigProvider()
	client, err := nosql.NewNosqlClientWithConfigurationProvider(conf)
	if err != nil {
		log.Fatal(err)
	}
	err = runExample(client, os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
}

func runExample(client nosql.NosqlClient, compartmentId string) error {
	// Creates a simple table with a LONG key and a single JSON field.
	tableName := "unko"
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ("+
		"cookie_id LONG, "+
		"audience_data JSON, "+
		"PRIMARY KEY(cookie_id))",
		tableName)
	createTableReq := nosql.CreateTableRequest{
		CreateTableDetails: nosql.CreateTableDetails{
			CompartmentId: common.String(compartmentId),
			Name:          common.String(tableName),
			DdlStatement:  common.String(stmt),
			TableLimits: &nosql.TableLimits{
				MaxReadUnits:    common.Int(50),
				MaxWriteUnits:   common.Int(50),
				MaxStorageInGBs: common.Int(1),
			},
		},
	}

	createTableRes, err := client.CreateTable(context.Background(), createTableReq)
	if err != nil {
		return fmt.Errorf("failed to create table %s: %v", tableName, err)
	}
	fmt.Println("Created table", tableName)

loop:
	for {
		getWorkRequestReq := nosql.GetWorkRequestRequest{
			WorkRequestId: createTableRes.OpcWorkRequestId,
		}
		getWorkRequestRes, err := client.GetWorkRequest(context.Background(), getWorkRequestReq)
		if err != nil {
			return fmt.Errorf("failed to get work request %s: %v", tableName, err)
		}
		switch getWorkRequestRes.Status {
		case nosql.WorkRequestStatusAccepted:
			time.Sleep(time.Second)
			continue
		case nosql.WorkRequestStatusInProgress:
			time.Sleep(time.Second)
			continue
		case nosql.WorkRequestStatusFailed:
			return fmt.Errorf("failed to create table %s: %v", tableName, err)
		case nosql.WorkRequestStatusSucceeded:
			break loop
		case nosql.WorkRequestStatusCanceling:
			time.Sleep(time.Second)
			continue
		case nosql.WorkRequestStatusCanceled:
			return fmt.Errorf("canceled to create table %s: %v", tableName, err)
		}
	}

	val := map[string]interface{}{
		"cookie_id": 123,
		"audience_data": map[string]interface{}{
			"ipaddr": "10.0.0.3",
			"audience_segment": map[string]interface{}{
				"sports_lover": "2018-11-30",
				"book_reader":  "2018-12-01",
			},
		},
	}
	updateRowReq := nosql.UpdateRowRequest{
		TableNameOrId: common.String(tableName),
		UpdateRowDetails: nosql.UpdateRowDetails{
			CompartmentId: common.String(compartmentId),
			Value:         val,
		},
	}
	_, err = client.UpdateRow(context.Background(), updateRowReq)
	if err != nil {
		return fmt.Errorf("failed to put a row: %v", err)
	}

	// Get the row
	getReq := nosql.GetRowRequest{
		CompartmentId: common.String(compartmentId),
		TableNameOrId: common.String(tableName),
		Key:           []string{"cookie_id:123"},
	}
	getRes, err := client.GetRow(context.Background(), getReq)
	if err != nil {
		return fmt.Errorf("failed to get a row: %v", err)
	}
	if getRes.Value != nil {
		json.NewEncoder(os.Stdout).Encode(getRes.Value)
		fmt.Println()
	}

	// QUERY the table. The table name is inferred from the query statement.
	query := fmt.Sprintf("select * from %s where cookie_id = 123", tableName)
	queryReq := nosql.QueryRequest{
		QueryDetails: nosql.QueryDetails{
			CompartmentId: common.String(compartmentId),
			Statement:     common.String(query),
			Consistency:   nosql.QueryDetailsConsistencyEventual,
		},
	}
	res, err := client.Query(context.Background(), queryReq)
	if err != nil {
		return fmt.Errorf("failed to execute query %q: %v", query, err)
	}
	for _, r := range res.Items {
		json.NewEncoder(os.Stdout).Encode(r)
		fmt.Println()
	}

	// Delete a row
	delReq := nosql.DeleteRowRequest{
		CompartmentId: common.String(compartmentId),
		TableNameOrId: common.String(tableName),
		Key:           []string{"cookie_id:123"},
	}
	delRes, err := client.DeleteRow(context.Background(), delReq)
	if err != nil {
		return fmt.Errorf("failed to delete a row: %v", err)
	}
	fmt.Printf("Deleted key: %v\nresult: %v\n", delReq.Key, delRes.DeleteRowResult)

	// Drop the table
	dropReq := nosql.DeleteTableRequest{
		CompartmentId: common.String(compartmentId),
		TableNameOrId: common.String(tableName),
	}
	_, err = client.DeleteTable(context.Background(), dropReq)
	if err != nil {
		return fmt.Errorf("failed to drop table %s: %v", tableName, err)
	}
	fmt.Println("Dropped table", tableName)

	return nil
}
