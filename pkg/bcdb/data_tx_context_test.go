// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"path"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.com/IBM-Blockchain/bcdb-server/pkg/server"
	"github.com/IBM-Blockchain/bcdb-server/pkg/server/testutils"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
)

func TestDataContext_PutAndGetKey(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	dbPerm := map[string]types.Privilege_Access{
		"bdb": 1,
	}
	addUser(t, "alice", adminSession, pemUserCert, dbPerm)
	userSession := openUserSession(t, bcdb, "alice", clientCertTemDir)

	putKeySync(t, "bdb", "key1", "value1", "alice", userSession)

	// Validate
	tx, err := userSession.DataTx()
	require.NoError(t, err)
	require.NotNil(t, tx)
	val, meta, err := tx.Get("bdb", "key1")
	require.NoError(t, err)
	require.EqualValues(t, []byte("value1"), val)
	require.NotNil(t, meta)
}

func TestDataContext_GetNonExistKey(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	dbPerm := map[string]types.Privilege_Access{
		"bdb": 1,
	}
	addUser(t, "alice", adminSession, pemUserCert, dbPerm)
	userSession := openUserSession(t, bcdb, "alice", clientCertTemDir)

	putKeySync(t, "bdb", "key1", "value1", "alice", userSession)

	tx, err := userSession.DataTx()
	require.NoError(t, err)
	res, meta, err := tx.Get("bdb", "key2")
	require.NoError(t, err)
	require.Nil(t, res)
	require.Nil(t, meta)
}

func TestDataContext_MultipleUpdateForSameKey(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	txDB, err := adminSession.DBsTx()
	require.NoError(t, err)

	err = txDB.CreateDB("testDB")
	require.NoError(t, err)

	txId, receipt, err := txDB.Commit(true)
	require.NoError(t, err)
	require.True(t, len(txId) > 0)
	require.NotNil(t, receipt)

	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	dbPerm := map[string]types.Privilege_Access{
		"bdb":    1,
		"testDB": 1,
	}
	addUser(t, "alice", adminSession, pemUserCert, dbPerm)
	userSession := openUserSession(t, bcdb, "alice", clientCertTemDir)
	putKeySync(t, "bdb", "key1", "value1", "alice", userSession)
	putKeySync(t, "testDB", "key2", "value2", "alice", userSession)

	acl := &types.AccessControl{
		ReadUsers:      map[string]bool{"alice": true},
		ReadWriteUsers: map[string]bool{"alice": true},
	}
	tx, err := userSession.DataTx()
	require.NoError(t, err)
	res1, meta, err := tx.Get("bdb", "key1")
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), res1)
	require.True(t, proto.Equal(acl, meta.GetAccessControl()))

	res2, meta, err := tx.Get("testDB", "key2")
	require.NoError(t, err)
	require.Equal(t, []byte("value2"), res2)
	require.True(t, proto.Equal(acl, meta.GetAccessControl()))

	err = tx.Put("bdb", "key1", []byte("value3"), acl)
	require.NoError(t, err)

	err = tx.Delete("testDB", "key2")
	require.NoError(t, err)

	dataTx, ok := tx.(*dataTxContext)
	require.True(t, ok)
	_, key1WriteExist := dataTx.operations["bdb"].dataWrites["key1"]
	_, key2WriteExist := dataTx.operations["testDB"].dataWrites["key2"]
	_, key1DeleteExist := dataTx.operations["bdb"].dataDeletes["key1"]
	_, key2DeleteExist := dataTx.operations["testDB"].dataDeletes["key2"]
	require.True(t, key1WriteExist)
	require.False(t, key2WriteExist)
	require.False(t, key1DeleteExist)
	require.True(t, key2DeleteExist)

	err = tx.Put("testDB", "key2", []byte("value4"), acl)
	require.NoError(t, err)

	err = tx.Delete("bdb", "key1")
	require.NoError(t, err)

	_, key1WriteExist = dataTx.operations["bdb"].dataWrites["key1"]
	_, key2WriteExist = dataTx.operations["testDB"].dataWrites["key2"]
	_, key1DeleteExist = dataTx.operations["bdb"].dataDeletes["key1"]
	_, key2DeleteExist = dataTx.operations["testDB"].dataDeletes["key2"]
	require.False(t, key1WriteExist)
	require.True(t, key2WriteExist)
	require.True(t, key1DeleteExist)
	require.False(t, key2DeleteExist)

	txID, _, err := tx.Commit(false)
	require.NoError(t, err)

	// Start another tx to query and make sure
	// results was successfully committed
	tx, err = userSession.DataTx()
	require.NoError(t, err)

	waitForTx(t, txID, userSession)

	err = tx.Delete("testDB", "key2")
	require.NoError(t, err)

	res, _, err := tx.Get("bdb", "key1")
	require.NoError(t, err)
	require.Nil(t, res)
}

func TestDataContext_CommitAbortFinality(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	dbPerm := map[string]types.Privilege_Access{
		"bdb": 1,
	}
	addUser(t, "alice", adminSession, pemUserCert, dbPerm)
	userSession := openUserSession(t, bcdb, "alice", clientCertTemDir)

	acl := &types.AccessControl{
		ReadUsers:      map[string]bool{"alice": true},
		ReadWriteUsers: map[string]bool{"alice": true},
	}

	for i := 0; i < 3; i++ {
		tx, err := userSession.DataTx()
		require.NoError(t, err)
		err = tx.Put("bdb", "key1", []byte("value1"), acl)
		require.NoError(t, err)

		assertTxFinality(t, TxFinality(i), tx, userSession)

		val, meta, err := tx.Get("bdb", "key")
		require.EqualError(t, err, ErrTxSpent.Error())
		require.Nil(t, val)
		require.Nil(t, meta)

		err = tx.Put("bdb", "key", []byte("value"), acl)
		require.EqualError(t, err, ErrTxSpent.Error())

		err = tx.Delete("bdb", "key")
		require.EqualError(t, err, ErrTxSpent.Error())

		if TxFinality(i) != TxFinalityAbort {
			tx, err := userSession.DataTx()
			require.NoError(t, err)
			val, meta, err := tx.Get("bdb", "key1")
			require.NoError(t, err)
			require.Equal(t, []byte("value1"), val)
			require.NotNil(t, meta)
		}
	}
}

func TestDataContext_MultipleGetForSameKeyInTxAndMVCCConflict(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	dbPerm := map[string]types.Privilege_Access{
		"bdb": 1,
	}
	addUser(t, "alice", adminSession, pemUserCert, dbPerm)
	userSession := openUserSession(t, bcdb, "alice", clientCertTemDir)

	putKeySync(t, "bdb", "key1", "value1", "alice", userSession)

	tx, err := userSession.DataTx()
	require.NoError(t, err)
	res, meta, err := tx.Get("bdb", "key1")
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), res)
	storedRead, ok := tx.(*dataTxContext).operations["bdb"].dataReads["key1"]
	require.True(t, ok)
	require.Equal(t, res, storedRead.GetValue())
	require.Equal(t, meta, storedRead.GetMetadata())

	putKeySync(t, "bdb", "key1", "value2", "alice", userSession)
	res, meta, err = tx.Get("bdb", "key1")
	require.NoError(t, err)
	storedReadUpdated, ok := tx.(*dataTxContext).operations["bdb"].dataReads["key1"]
	require.True(t, ok)
	require.Equal(t, res, storedRead.GetValue())
	require.Equal(t, meta, storedRead.GetMetadata())
	require.Equal(t, storedReadUpdated, storedRead)
	require.NoError(t, err)
	_, receipt, err := tx.Commit(true)
	require.NoError(t, err)
	require.NotNil(t, receipt)
	require.Equal(t, receipt.GetHeader().GetValidationInfo()[int(receipt.GetTxIndex())].GetFlag(), types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE)
}

func TestDataContext_GetUserPermissions(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice", "bob", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	dbPerm := map[string]types.Privilege_Access{
		"bdb": 1,
	}
	addUser(t, "alice", adminSession, pemUserCert, dbPerm)
	aliceSession := openUserSession(t, bcdb, "alice", clientCertTemDir)

	putKeySync(t, "bdb", "key1", "value1", "alice", aliceSession)

	pemUserCert, err = ioutil.ReadFile(path.Join(clientCertTemDir, "bob.pem"))
	require.NoError(t, err)
	addUser(t, "bob", adminSession, pemUserCert, dbPerm)
	bobSession := openUserSession(t, bcdb, "bob", clientCertTemDir)
	tx, err := bobSession.DataTx()
	require.NoError(t, err)
	_, _, err = tx.Get("bdb", "key1")
	require.Error(t, err)
	require.EqualError(t, err, "error handling request, server returned: status: 403 Forbidden, message: error while processing 'GET /data/bdb/key1' because the user [bob] has no permission to read key [key1] from database [bdb]")
	err = tx.Abort()
	require.NoError(t, err)

	txUpdateUser, err := aliceSession.DataTx()
	require.NoError(t, err)
	acl := &types.AccessControl{
		ReadUsers:      map[string]bool{"alice": true, "bob": true},
		ReadWriteUsers: map[string]bool{"alice": true},
	}
	err = txUpdateUser.Put("bdb", "key1", []byte("value2"), acl)
	require.NoError(t, err)

	txID, _, err := txUpdateUser.Commit(false)
	require.NoError(t, err)
	waitForTx(t, txID, aliceSession)
	validateValue(t, "key1", "value2", aliceSession)

	tx, err = bobSession.DataTx()
	require.NoError(t, err)
	bobVal, meta, err := tx.Get("bdb", "key1")
	require.NoError(t, err)
	require.EqualValues(t, []byte("value2"), bobVal)
	require.True(t, proto.Equal(meta.GetAccessControl(), acl))
}

func TestDataContext_GetTimeout(t *testing.T) {
	clientCertTemDir := testutils.GenerateTestClientCrypto(t, []string{"admin", "alice", "bob", "server"})
	testServer, _, _, err := SetupTestServer(t, clientCertTemDir)
	defer testServer.Stop()
	require.NoError(t, err)
	testServer.Start()

	bcdb, adminSession := connectAndOpenAdminSession(t, testServer, clientCertTemDir)
	pemUserCert, err := ioutil.ReadFile(path.Join(clientCertTemDir, "alice.pem"))
	require.NoError(t, err)
	dbPerm := map[string]types.Privilege_Access{
		"bdb": 1,
	}
	addUser(t, "alice", adminSession, pemUserCert, dbPerm)
	sessionNoTimeout := openUserSession(t, bcdb, "alice", clientCertTemDir)
	sessionOneNanoTimeout := openUserSessionWithQueryTimeout(t, bcdb, "alice", clientCertTemDir, time.Nanosecond)
	sessionTenSecondTimeout := openUserSessionWithQueryTimeout(t, bcdb, "alice", clientCertTemDir, time.Second*10)

	putKeySync(t, "bdb", "key1", "value1", "alice", sessionNoTimeout)

	tx1, err := sessionOneNanoTimeout.DataTx()
	require.NoError(t, err)
	require.NotNil(t, tx1)
	val, _, err := tx1.Get("bdb", "key1")
	require.Error(t, err)
	require.Nil(t, val)
	require.Contains(t, err.Error(), "queryTimeout error")

	tx2, err := sessionTenSecondTimeout.DataTx()
	require.NoError(t, err)
	require.NotNil(t, tx2)
	val, _, err = tx2.Get("bdb", "key1")
	require.NoError(t, err)
	require.EqualValues(t, []byte("value1"), val)
}

func connectAndOpenAdminSession(t *testing.T, testServer *server.BCDBHTTPServer, cryptoDir string) (BCDB, DBSession) {
	serverPort, err := testServer.Port()
	require.NoError(t, err)
	// Create new connection
	bcdb := createDBInstance(t, cryptoDir, serverPort)
	// New session with admin user context
	session := openUserSession(t, bcdb, "admin", cryptoDir)

	return bcdb, session
}

func addUser(t *testing.T, userName string, session DBSession, pemUserCert []byte, dbPerm map[string]types.Privilege_Access) {
	tx, err := session.UsersTx()
	require.NoError(t, err)

	certBlock, _ := pem.Decode(pemUserCert)
	err = tx.PutUser(&types.User{
		ID:          userName,
		Certificate: certBlock.Bytes,
		Privilege: &types.Privilege{
			DBPermission: dbPerm,
		},
	}, nil)
	require.NoError(t, err)
	_, receipt, err := tx.Commit(true)
	require.NoError(t, err)
	require.NotNil(t, receipt)

	tx, err = session.UsersTx()
	require.NoError(t, err)
	user, err := tx.GetUser(userName)
	require.NoError(t, err)
	require.Equal(t, userName, user.GetID())
}

func putKeySync(t *testing.T, dbName, key string, value string, user string, session DBSession) {
	tx, err := session.DataTx()
	require.NoError(t, err)

	readUsers := make(map[string]bool)
	readWriteUsers := make(map[string]bool)

	readUsers[user] = true
	readWriteUsers[user] = true

	err = tx.Put(dbName, key, []byte(value), &types.AccessControl{
		ReadUsers:      readUsers,
		ReadWriteUsers: readWriteUsers,
	})
	require.NoError(t, err)

	txID, receipt, err := tx.Commit(true)
	require.NoError(t, err, fmt.Sprintf("Key = %s, value = %s", key, value))
	require.NotNil(t, txID)
	require.NotNil(t, receipt)
}

func putMultipleKeysAndValues(t *testing.T, key []string, value []string, user string, session DBSession) (txEnvelopes []proto.Message) {
	return putMultipleKeysAndValidateMultipleUsers(t, key, value, []string{user}, session)
}

func putMultipleKeysAndValidateMultipleUsers(t *testing.T, key []string, value []string, users []string, session DBSession) (txEnvelopes []proto.Message) {
	// Creating new key
	var txId string
	for i := 0; i < len(key); i++ {
		tx, err := session.DataTx()
		require.NoError(t, err)

		readUsers := make(map[string]bool)
		readWriteUsers := make(map[string]bool)
		for _, user := range users {
			readUsers[user] = true
			readWriteUsers[user] = true
		}
		err = tx.Put("bdb", key[i], []byte(value[i]), &types.AccessControl{
			ReadUsers:      readUsers,
			ReadWriteUsers: readWriteUsers,
		})
		require.NoError(t, err)

		txId, _, err = tx.Commit(false)
		require.NoError(t, err, fmt.Sprintf("Key = %s, value = %s", key[i], value[i]))
		txEnv, err := tx.TxEnvelope()
		require.NoError(t, err)
		txEnvelopes = append(txEnvelopes, txEnv)
	}

	waitForTx(t, txId, session)
	return txEnvelopes
}

func validateValue(t *testing.T, key string, value string, session DBSession) {
	// Start another tx to query and make sure
	// results was successfully committed
	tx, err := session.DataTx()
	require.NoError(t, err)
	val, _, err := tx.Get("bdb", key)
	require.NoError(t, err)
	require.Equal(t, val, []byte(value))
}

func waitForTx(t *testing.T, txID string, session DBSession) {
	l, err := session.Ledger()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		r, err := l.GetTransactionReceipt(txID)

		return err == nil && r != nil && r.GetHeader() != nil &&
			uint64(len(r.GetHeader().GetValidationInfo())) > r.GetTxIndex()
	}, time.Minute, 200*time.Millisecond)
}
