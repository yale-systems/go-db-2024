package godb

import (
	"os"
	"path/filepath"

	// Imports all sub-components
	"mit.edu/dsg/godb/catalog"
	"mit.edu/dsg/godb/execution"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/logging"
	"mit.edu/dsg/godb/recovery"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/transaction"
)

// GoDB is the top-level container for the database system.
type GoDB struct {
	Catalog            *catalog.Catalog
	BufferPool         *storage.BufferPool
	TableManager       *execution.TableManager
	LogManager         logging.LogManager
	TransactionManager *transaction.TransactionManager
	LockManager        *transaction.LockManager
	IndexManager       *indexing.IndexManager
}

func NewGoDB(catalog *catalog.Catalog, storageDir, logDir string, bufferPoolSize int) (*GoDB, error) {
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, err
	}

	logManager, err := logging.NewDoubleBufferLogManager(filepath.Join(logDir, "godb.log"))
	if err != nil {
		return nil, err
	}

	bufferPool := storage.NewBufferPool(bufferPoolSize, storage.NewDiskStorageManager(storageDir))
	lockManager := transaction.NewLockManager()
	transactionManager := transaction.NewTransactionManager(logManager, bufferPool, lockManager)
	tableManager, err := execution.NewTableManager(catalog, bufferPool, logManager, lockManager)
	if err != nil {
		return nil, err
	}
	indexManager, err := indexing.NewIndexManager(catalog)

	recoveryManager := recovery.NewRecoveryManager(logManager, bufferPool, transactionManager, filepath.Join(logDir, "godb.log"), catalog, tableManager, indexManager)
	if err := recoveryManager.Recover(); err != nil {
		return nil, err
	}

	return &GoDB{
		Catalog:            catalog,
		BufferPool:         bufferPool,
		TableManager:       tableManager,
		LogManager:         logManager,
		TransactionManager: transactionManager,
		LockManager:        lockManager,
		IndexManager:       indexManager,
	}, nil
}
