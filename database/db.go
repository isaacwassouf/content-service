package database

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
)

type ContentManagementServiceDB struct {
	Db *sql.DB
}

func NewContentManagementService() (*ContentManagementServiceDB, error) {
	// read the environment variables
	user := os.Getenv("MYSQL_USER")
	pass := os.Getenv("MYSQL_PASSWORD")
	host := os.Getenv("MYSQL_HOST")
	port := os.Getenv("MYSQL_PORT")
	name := os.Getenv("MYSQL_DATABASE")

	db, err := sql.Open(
		"mysql",
		fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, pass, host, port, name),
	)
	if err != nil {
		return nil, err
	}
	return &ContentManagementServiceDB{Db: db}, nil
}
