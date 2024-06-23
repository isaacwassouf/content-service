package utils

import (
	"database/sql"
	"fmt"
	"github.com/joho/godotenv"
)

func LoadEnvVarsFromFile() error {
	err := godotenv.Load()
	if err != nil {
		return err
	}
	return nil
}

func CheckTableExists(db *sql.DB, tableName string) (bool, error) {
	query := fmt.Sprintf("SHOW TABLES LIKE '%s'", tableName)
	rows, err := db.Query(query)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	return rows.Next(), nil
}

func CheckColumnExists(db *sql.DB, tableName string, columnName string) (bool, error) {
	query := fmt.Sprintf("SHOW COLUMNS FROM %s LIKE '%s'", tableName, columnName)
	rows, err := db.Query(query)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	return rows.Next(), nil
}
