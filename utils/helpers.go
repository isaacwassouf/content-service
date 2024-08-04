package utils

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

func LoadEnvVarsFromFile() error {
	environment := GetGoEnv()
	if environment == "development" {
		err := godotenv.Load()
		if err != nil {
			return err
		}
	}
	return nil
}

func GetGoEnv() string {
	environment, found := os.LookupEnv("GO_ENV")
	if !found {
		return "development"
	}
	return environment
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
