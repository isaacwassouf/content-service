package main

import (
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"net"
	"strings"

	db "github.com/isaacwassouf/content-service/database"
	pb "github.com/isaacwassouf/content-service/protobufs/content_management_service"
	"github.com/isaacwassouf/content-service/utils"
)

type ContentManagementService struct {
	pb.UnimplementedContentServiceServer
	contentManagementServiceDB *db.ContentManagementServiceDB
}

func (s *ContentManagementService) GetContent(ctx context.Context, in *pb.GetContentRequest) (*pb.GetContentResponse, error) {
	// check if the table exists
	tableExists, err := utils.CheckTableExists(s.contentManagementServiceDB.Db, in.TableName)
	if err != nil {
		return nil, status.Error(codes.Internal, "Failed to check if table exists")
	}
	if !tableExists {
		return nil, status.Error(codes.NotFound, "Table does not exist")
	}

	query := fmt.Sprintf("SELECT * FROM %s WHERE id = ? LIMIT 1", in.TableName)
	rows, err := s.contentManagementServiceDB.Db.Query(query, in.EntityId)
	if err != nil {
		return nil, status.Error(codes.Internal, "Failed to query the database")
	}

	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, status.Error(codes.Internal, "Failed to get columns")
	}

	data := make(map[string]interface{}, len(cols))
	columns := make([]interface{}, len(cols))
	columnPointers := make([]interface{}, len(cols))

	for i := range columns {
		columnPointers[i] = &columns[i]
	}

	if rows.Next() {
		_ = rows.Scan(columnPointers...)

		for i, colName := range cols {
			val := columnPointers[i].(*interface{})
			// Check if the value is a byte slice and convert it to a string
			if b, ok := (*val).([]byte); ok {
				data[colName] = string(b)
			} else {
				data[colName] = *val
			}
		}
	} else {
		return nil, status.Error(codes.NotFound, "Entity not found")
	}

	// convert the data to bytes
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return nil, status.Error(codes.Internal, "Failed to marshal data")
	}

	return &pb.GetContentResponse{Content: dataBytes}, nil
}

func (s *ContentManagementService) DeleteContent(ctx context.Context, in *pb.DeleteContentRequest) (*pb.DeleteContentResponse, error) {
	// check if the table exists
	tableExists, err := utils.CheckTableExists(s.contentManagementServiceDB.Db, in.TableName)
	if err != nil {
		return nil, status.Error(codes.Internal, "Failed to check if table exists")
	}
	if !tableExists {
		return nil, status.Error(codes.NotFound, "Table does not exist")
	}
	// format the query
	query := fmt.Sprintf("DELETE FROM %s WHERE id = ?", in.TableName)
	// execute the query
	result, err := s.contentManagementServiceDB.Db.Exec(query, in.EntityId)
	if err != nil {
		return nil, status.Error(codes.Internal, "Failed to delete entity")
	}
	// check if the entity was deleted
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, status.Error(codes.Internal, "Failed to get rows affected")
	}
	if rowsAffected == 0 {
		return nil, status.Error(codes.NotFound, "Entity not found")
	}

	return &pb.DeleteContentResponse{Message: "Deleted the entity successfully"}, nil
}

func (s *ContentManagementService) CreateContent(ctx context.Context, in *pb.CreateContentRequest) (*pb.CreateContentResponse, error) {
	tableExists, err := utils.CheckTableExists(s.contentManagementServiceDB.Db, in.TableName)
	if err != nil {
		return nil, status.Error(codes.Internal, "Failed to check if table exists")
	}
	if !tableExists {
		return nil, status.Error(codes.NotFound, "Table does not exist")
	}

	var columns []string
	var placeholders []string
	var values []interface{}

	for col, val := range in.Data {
		columns = append(columns, col)
		placeholders = append(placeholders, "?")
		values = append(values, val)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", in.TableName, strings.Join(columns, ","), strings.Join(placeholders, ","))

	result, err := s.contentManagementServiceDB.Db.Exec(query, values...)
	if err != nil {
		return nil, status.Error(codes.Internal, "Failed to insert entity")
	}

	id, err := result.LastInsertId()
	if err != nil {
		return nil, status.Error(codes.Internal, "Failed to get last insert ID")
	}

	return &pb.CreateContentResponse{Id: id, Message: fmt.Sprintf("Created entity with id %d", id)}, nil
}

func main() {
	// load the environment variables from the .env file
	err := utils.LoadEnvVarsFromFile()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	// Create a new schemaManagementServiceDB
	contentManagementServiceDB, err := db.NewContentManagementService()
	if err != nil {
		log.Fatalf("failed to create a new SchemaManagementServiceDB: %v", err)
	}
	// ping the database
	err = contentManagementServiceDB.Db.Ping()
	if err != nil {
		log.Fatalf("failed to ping the database: %v", err)
	}

	// Start the server
	ls, err := net.Listen("tcp", ":8085")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterContentServiceServer(s, &ContentManagementService{
		contentManagementServiceDB: contentManagementServiceDB,
	})

	log.Printf("Server listening at %v", ls.Addr())

	if err := s.Serve(ls); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
