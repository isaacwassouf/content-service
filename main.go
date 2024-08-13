package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/Masterminds/squirrel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

	var sql string
	var args []interface{}
	if in.CreatorId == 0 {
		sql, args, err = squirrel.Select("*").From(in.TableName).Where(squirrel.Eq{"id": in.EntityId}).ToSql()
		if err != nil {
			return nil, status.Error(codes.Internal, "Failed to build SQL query")
		}

	} else {
		sql, args, err = squirrel.Select("*").From(in.TableName).Where(squirrel.Eq{"id": in.EntityId, "creator_id": in.CreatorId}).ToSql()
		if err != nil {
			return nil, status.Error(codes.Internal, "Failed to build SQL query")
		}
	}

	rows, err := s.contentManagementServiceDB.Db.Query(sql, args...)
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

	var query string
	var result sql.Result
	if in.CreatorId == 0 {
		query = fmt.Sprintf("DELETE FROM %s WHERE id = ?", in.TableName)

		result, err = s.contentManagementServiceDB.Db.Exec(query, in.EntityId)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	} else {
		query = fmt.Sprintf("DELETE FROM %s WHERE id = ? AND creator_id = ?", in.TableName)

		result, err = s.contentManagementServiceDB.Db.Exec(query, in.EntityId, in.CreatorId)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
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

	// add the creator_id to the columns and VALUES
	columns = append(columns, "creator_id")
	placeholders = append(placeholders, "?")
	values = append(values, in.CreatorId)

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

func (s *ContentManagementService) UpdateContent(ctx context.Context, in *pb.UpdateContentRequest) (*pb.UpdateContentResponse, error) {
	tableExists, err := utils.CheckTableExists(s.contentManagementServiceDB.Db, in.TableName)
	if err != nil {
		return nil, status.Error(codes.Internal, "Failed to check if table exists")
	}
	if !tableExists {
		return nil, status.Error(codes.NotFound, "Table does not exist")
	}

	var columns []string
	var values []interface{}

	for col, val := range in.Data {
		columns = append(columns, fmt.Sprintf("%s = ?", col))
		values = append(values, val)
	}

	var query string
	var result sql.Result

	if in.CreatorId == 0 {
		query = fmt.Sprintf("UPDATE %s SET %s WHERE id = ?", in.TableName, strings.Join(columns, ","))

		result, err = s.contentManagementServiceDB.Db.Exec(query, append(values, in.EntityId)...)
		if err != nil {
			return nil, status.Error(codes.Internal, "Failed to update entity")
		}

	} else {
		query = fmt.Sprintf("UPDATE %s SET %s WHERE id = ? AND creator_id = ?  ", in.TableName, strings.Join(columns, ","))

		result, err = s.contentManagementServiceDB.Db.Exec(query, append(values, in.EntityId, in.CreatorId)...)
		if err != nil {
			return nil, status.Error(codes.Internal, "Failed to update entity")
		}
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, status.Error(codes.Internal, "Failed to get rows affected")
	}
	if rowsAffected == 0 {
		return nil, status.Error(codes.NotFound, "Entity not found")
	}

	return &pb.UpdateContentResponse{Message: "Updated the entity successfully"}, nil
}

func (s *ContentManagementService) ListContent(ctx context.Context, in *pb.ListContentRequest) (*pb.ListContentResponse, error) {
	// check if the table exists
	tableExists, err := utils.CheckTableExists(s.contentManagementServiceDB.Db, in.TableName)
	if err != nil {
		return nil, status.Error(codes.Internal, "Failed to check if table exists")
	}
	if !tableExists {
		return nil, status.Error(codes.NotFound, "Table does not exist")
	}

	// get the total number of rows
	var totalRows int
	totalRowsQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", in.TableName)
	err = s.contentManagementServiceDB.Db.QueryRow(totalRowsQuery).Scan(&totalRows)
	if err != nil {
		return nil, status.Error(codes.Internal, "Failed to get total rows")
	}

	// get the total number of pages
	totalPages := totalRows / int(in.PerPage)
	if totalRows%int(in.PerPage) != 0 {
		totalPages++
	}

	// init the sqlBuilder
	sqlBuilder := squirrel.Select("*").
		From(in.TableName).
		Limit(uint64(in.PerPage)).
		Offset(uint64(in.PerPage * (in.Page - 1)))

		// apply the creator_id filter
	if in.CreatorId != 0 {
		sqlBuilder = sqlBuilder.Where(squirrel.Eq{"creator_id": in.CreatorId})
	}

	// apply the filters, i.e. WHERE col = val if filters are provided
	if in.Filters != nil {
		for col, val := range in.Filters {
			sqlBuilder = sqlBuilder.Where(squirrel.Eq{col: val})
		}
	}

	sql, args, err := sqlBuilder.ToSql()
	if err != nil {
		return nil, status.Error(codes.Internal, "Failed to build SQL query")
	}

	rows, err := s.contentManagementServiceDB.Db.Query(sql, args...)
	if err != nil {
		return nil, status.Error(codes.Internal, "Failed to query the database")
	}

	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, status.Error(codes.Internal, "Failed to get columns")
	}

	var entities [][]byte

	for rows.Next() {
		columnPointers := make([]interface{}, len(cols))
		columns := make([]interface{}, len(cols))

		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			return nil, status.Error(codes.Internal, "Failed to scan row")
		}

		data := make(map[string]interface{}, len(cols))
		for i, colName := range cols {
			val := columnPointers[i].(*interface{})
			// Check if the value is a byte slice and convert it to a string
			if b, ok := (*val).([]byte); ok {
				data[colName] = string(b)
			} else {
				data[colName] = *val
			}
		}

		dataBytes, err := json.Marshal(data)
		if err != nil {
			return nil, status.Error(codes.Internal, "Failed to marshal data")
		}

		entities = append(entities, dataBytes)
	}

	return &pb.ListContentResponse{Page: in.Page, PerPage: in.PerPage, TotalPages: int32(totalPages), Entities: entities}, nil
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
