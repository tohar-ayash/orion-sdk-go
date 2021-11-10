package main

import (
	"fmt"
	"github.com/hyperledger-labs/orion-sdk-go/examples/util"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/config"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
	"os"
)

/*
	- Creating database 'db' with 3 indexes associated with it (name, age, gender)
	- Adding multiple key, value pairs to the database
	- ////
*/
func main() {
	if err := executeJsonQueryExample("../../util/config.yml"); err != nil {
		os.Exit(1)
	}
}

func executeJsonQueryExample(configLocation string) error {
	session, err := prepareData(configLocation)
	if session == nil || err != nil {
		return err
	}

	err = clearData(session)
	if err != nil {
		return err
	}

	err = createDatabase(session)
	if err != nil {
		return err
	}

	err = insertData(session)
	if err != nil {
		return err
	}

	err = validQuery(session)
	if err != nil {
		return err
	}

	return nil
}

func prepareData(configLocation string) (bcdb.DBSession, error) {
	c, err := util.ReadConfig(configLocation)
	if err != nil {
		fmt.Printf(err.Error())
		return nil, err
	}

	logger, err := logger.New(
		&logger.Config{
			Level:         "debug",
			OutputPath:    []string{"stdout"},
			ErrOutputPath: []string{"stderr"},
			Encoding:      "console",
			Name:          "bcdb-client",
		},
	)
	if err != nil {
		fmt.Printf(err.Error())
		return nil, err
	}

	conConf := &config.ConnectionConfig{
		ReplicaSet: c.ConnectionConfig.ReplicaSet,
		RootCAs:    c.ConnectionConfig.RootCAs,
		Logger:     logger,
	}

	fmt.Println("Opening connection to database, configuration: ", c.ConnectionConfig)
	db, err := bcdb.Create(conConf)
	if err != nil {
		fmt.Printf("Database connection creating failed, reason: %s\n", err.Error())
		return nil, err
	}

	sessionConf := &config.SessionConfig{
		UserConfig:   c.SessionConfig.UserConfig,
		TxTimeout:    c.SessionConfig.TxTimeout,
		QueryTimeout: c.SessionConfig.QueryTimeout}

	fmt.Println("Opening session to database, configuration: ", c.SessionConfig)
	session, err := db.Session(sessionConf)
	if err != nil {
		fmt.Printf("Database session creating failed, reason: %s\n", err.Error())
		return nil, err
	}

	return session, nil
}

func clearData(session bcdb.DBSession) error{
	fmt.Println("Opening database transaction")
	dbTx, err := session.DBsTx()
	if err != nil {
		fmt.Printf("Database transaction creation failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Checking if database 'db' already exists")
	exists, err := dbTx.Exists("db")
	if exists {
		fmt.Println("Deleting database 'db'")
		err = dbTx.DeleteDB("db")
		if err != nil {
			fmt.Printf("Deleting database failed, reason: %s\n", err.Error())
			return err
		}
	}

	fmt.Println("Committing transaction")
	txID, _, err := dbTx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	return nil
}

func createDatabase(session bcdb.DBSession) error{
	fmt.Println("Opening database transaction")
	dbTx, err := session.DBsTx()
	if err != nil {
		fmt.Printf("Database transaction creation failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Creating database 'db' with name, age and gender as indexes")
	index := map[string]types.IndexAttributeType{
		"name": types.IndexAttributeType_STRING,
		"age": types.IndexAttributeType_NUMBER,
		"gender": types.IndexAttributeType_BOOLEAN, //female 1, male 0
	}
	err = dbTx.CreateDB("db", index)
	if err != nil {
		fmt.Printf("Database creating failed, reason: %s\n", err.Error())
		return err
	}

	fmt.Println("Committing transaction")
	txID, _, err := dbTx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	return nil
}

func insertData(session bcdb.DBSession) error{
	fmt.Println("Opening data transaction")
	tx, err := session.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	v0 := `{"name": "alice", "age": 20, "gender": true, education: "high-school"}`
	v1 := `{"name": "bob", "age": 30, "gender": false, education: "bachelor"}`
	v2 := `{"name": "charlie", "age": 40, "gender": false, education: "master"}`
	v3 := `{"name": "dan", "age": 20, "gender": false, education: "doctorate"}`
	v4 := `{"name": "eve", "age": 30, "gender": true, education: "bachelor"}`
	v5 := `{"name": "alice", "age": 30, "gender": true, education: "bachelor"}`
	v6 := `{"name": "bob", "age": 40, "gender": false, education: "master"}`
	v7 := `{"name": "charlie", "age": 20, "gender": true, education: "high-school"}`
	v8 := `{"name": "dan", "age": 30, "gender": false, education: "master"}`
	v9 := `{"name": "eve", "age":40 , "gender": true, education: "doctorate"}`

	values := [10]string{v0, v1, v2, v3, v4, v5, v6, v7, v8,v9}
	keys := [10]string{"id0", "id1", "id2", "id3", "id4", "id5", "id6", "id7", "id8", "id9"}

	for i:=0; i<10; i++{
		fmt.Println("Adding key, value: " + keys[i] + ", "+ values[i] +" to the database")
		err = tx.Put("db", keys[i], []byte(values[i]), nil)
		if err != nil {
			fmt.Printf("Adding new key to database failed, reason: %s\n", err.Error())
			return err
		}
	}

	fmt.Println("Committing transaction")
	txID, _, err := tx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)

	//check data existence
	fmt.Println("Opening data transaction")
	tx, err = session.DataTx()
	if err != nil {
		fmt.Printf("Data transaction creating failed, reason: %s\n", err.Error())
		return err
	}

	for i:=0; i<10; i++{
		val, _, err := tx.Get("db", keys[i])
		if err != nil {
			fmt.Printf("Adding new key to database failed, reason: %s\n", err.Error())
			return err
		}
		fmt.Println("key " + keys[i] + " value is " + string(val))
	}

	fmt.Println("Committing transaction")
	txID, _, err = tx.Commit(true)
	if err != nil {
		fmt.Printf("Commit failed, reason: %s\n", err.Error())
		return err
	}
	fmt.Printf("Transaction number %s committed successfully\n", txID)




	return nil
}

func validQuery(session bcdb.DBSession) error{
	q, err := session.JSONQuery()
	if err != nil {
		fmt.Printf("Failed to return handler to access bcdb data through JSON query, reason: %s\n", err.Error() )
		return err
	}

	query1 := `
	{
		"selector": {
				"age": {
					"$eq": 30
				}
		}
	}
	`

	kvs, err := q.Execute("db", query1)
	if err != nil {
		fmt.Printf("Failed to execute JSON query, reason: %s\n", err.Error() )
		return err
	}
	if kvs == nil {
		println("kvs nil")
		return errors.New("kvs nil")
	}

	return nil
}

func invalidQuery(){

}
