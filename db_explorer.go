package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"strconv"
	"strings"
)

type SR map[string]interface{}

func contains(arr []string, str string) bool {
	for _, a := range arr {
		if a == str {
			return true
		}
	}
	return false
}

func getTableList(db *sql.DB) ([]string, error) {
	var tableNames []string
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return tableNames, err
	}
	defer rows.Close()
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return tableNames, err
		}
		tableNames = append(tableNames, tableName)

	}
	return tableNames, nil
}

func getRowsList(db *sql.DB, tableName string, offset int, limit int) ([]SR, error) {
	var records []SR
	rows, err := db.Query(fmt.Sprintf(`SELECT * FROM %s LIMIT ? OFFSET ?`, tableName), limit, offset)
	if err != nil {
		return records, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return records, err
	}
	items := make([]interface{}, len(columns))
	itemsRelated := make([]interface{}, len(columns))
	for rows.Next() {
		record := SR{}
		for i := range columns {
			items[i] = &itemsRelated[i]
		}
		if err := rows.Scan(items...); err != nil {
			return records, err
		}
		for i, column := range columns {
			if itemsRelated[i] == nil {
				record[column] = nil
				continue
			}
			intValue64, ok := itemsRelated[i].(int64)
			if ok {
				record[column] = intValue64
				continue
			}
			intValue32, ok := itemsRelated[i].(int32)
			if ok {
				record[column] = intValue32
				continue
			}
			byteValue, ok := itemsRelated[i].([]byte)
			if ok {
				record[column] = string(byteValue)
				continue
			}
			floatValue64, ok := itemsRelated[i].(float64)
			if ok {
				record[column] = floatValue64
				continue
			}
			floatValue32, ok := itemsRelated[i].(float32)
			if ok {
				record[column] = floatValue32
				continue
			}
		}
		records = append(records, record)
	}
	return records, nil
}

func getTypesForColumns(db *sql.DB, tableName string) (map[string]string, error) {
	rows, err := db.Query(fmt.Sprintf(`SHOW FULL COLUMNS FROM %s`, tableName))
	result := make(map[string]string)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			Field      string
			Type       string
			Collation  interface{}
			Null       string
			Key        interface{}
			Default    interface{}
			Extra      interface{}
			Privileges interface{}
			Comment    interface{}
		)
		if err := rows.Scan(&Field, &Type, &Collation, &Null, &Key, &Default, &Extra, &Privileges, &Comment); err != nil {
			return nil, err
		}
		result[Field] = fmt.Sprintf(`%[1]s,%[2]s`, Type, Null)

	}
	return result, err
}

func getPrimaryColumnName(db *sql.DB, tableName string) (string, error) {
	rows, err := db.Query(fmt.Sprintf(`SHOW FULL COLUMNS FROM %s`, tableName))
	if err != nil {
		return "", err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			Field      string
			Type       interface{}
			Collation  interface{}
			Null       interface{}
			Key        string
			Default    interface{}
			Extra      interface{}
			Privileges interface{}
			Comment    interface{}
		)
		if err := rows.Scan(&Field, &Type, &Collation, &Null, &Key, &Default, &Extra, &Privileges, &Comment); err != nil {
			return "", err
		}
		if Key != "PRI" {
			continue
		}
		return Field, nil
	}
	return "", err
}

func getRowDetail(db *sql.DB, tableName string, rowId int) (SR, error) {
	columnNamePK, err := getPrimaryColumnName(db, tableName)
	if err != nil {
		return nil, err
	}
	var records []SR
	rows, err := db.Query(fmt.Sprintf(`SELECT * FROM %[1]s WHERE %[2]s = ?`, tableName, columnNamePK), rowId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	items := make([]interface{}, len(columns))
	itemsRelated := make([]interface{}, len(columns))
	for rows.Next() {
		record := SR{}
		for i := range columns {
			items[i] = &itemsRelated[i]
		}
		if err := rows.Scan(items...); err != nil {
			return nil, err
		}
		for i, column := range columns {
			if itemsRelated[i] == nil {
				record[column] = nil
				continue
			}
			intValue64, ok := itemsRelated[i].(int64)
			if ok {
				record[column] = intValue64
				continue
			}
			intValue32, ok := itemsRelated[i].(int32)
			if ok {
				record[column] = intValue32
				continue
			}
			floatValue64, ok := itemsRelated[i].(float64)
			if ok {
				record[column] = floatValue64
				continue
			}
			floatValue32, ok := itemsRelated[i].(float32)
			if ok {
				record[column] = floatValue32
				continue
			}
			byteValue, ok := itemsRelated[i].([]byte)
			if ok {
				record[column] = string(byteValue)
				continue
			}
		}
		records = append(records, record)
	}
	if len(records) == 0 {
		return nil, nil
	}
	return records[0], nil
}

func tableListHandler(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	tableNames, err := getTableList(db)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	responseJson, _ := json.Marshal(SR{
		"response": SR{
			"tables": tableNames,
		},
	})
	w.Write(responseJson)
}

func tableDetailHandler(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	tableNames, err := getTableList(db)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	reTableName := regexp.MustCompile(`(?P<tablename>\w+)`)
	matchStrings := reTableName.FindStringSubmatch(r.URL.Path)
	if len(matchStrings) > 0 {
		currTableName := matchStrings[0]
		if !contains(tableNames, currTableName) {
			w.WriteHeader(http.StatusNotFound)
			responseJson, _ := json.Marshal(SR{
				"error": "unknown table",
			})
			w.Write(responseJson)
			return
		}
		limitParam := r.FormValue("limit")
		offsetParam := r.FormValue("offset")
		limit := 5
		offset := 0
		if limitParam != "" {
			levelParam64, levelParamErr := strconv.ParseInt(limitParam, 10, 64)
			if levelParamErr != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			limit = int(levelParam64)
		}
		if offsetParam != "" {
			offsetParam64, offsetParamErr := strconv.ParseInt(offsetParam, 10, 64)
			if offsetParamErr != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			offset = int(offsetParam64)
		}
		SRList, err := getRowsList(db, currTableName, offset, limit)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		responseJson, _ := json.Marshal(SR{
			"response": SR{
				"records": SRList,
			},
		})
		w.Write(responseJson)
		return
	}
	w.WriteHeader(http.StatusInternalServerError)
	return
}

func findInvalidTypeField(bodyMap map[string]interface{}, typesForColumns map[string]string) (string, error) {
	for bodyKey, bodyValue := range bodyMap {
		typeValueFromDB := typesForColumns[bodyKey]
		_, okByte := bodyValue.(string)
		_, okInt32 := bodyValue.(int32)
		_, okInt64 := bodyValue.(int64)
		_, okFloat32 := bodyValue.(float32)
		_, okFloat64 := bodyValue.(float64)
		if (bodyValue == nil && !strings.Contains(typeValueFromDB, "YES")) ||
			(strings.Contains(typeValueFromDB, "int") && !(okInt32 || okInt64)) ||
			(strings.Contains(typeValueFromDB, "float") && !(okFloat32 || okFloat64)) ||
			((strings.Contains(typeValueFromDB, "text") || strings.Contains(typeValueFromDB, "varchar")) && !okByte) {
			return bodyKey, errors.New("invalid type field")

		}
	}
	return "", nil
}

func rowCreateHandler(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	tableNames, err := getTableList(db)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	reTableName := regexp.MustCompile(`(?P<tablename>\w+)`)
	matchStrings := reTableName.FindAllString(r.URL.Path, 1)
	if len(matchStrings) > 0 {
		currTableName := matchStrings[0]
		if !contains(tableNames, currTableName) {
			w.WriteHeader(http.StatusNotFound)
			responseJson, _ := json.Marshal(SR{
				"error": "unknown table",
			})
			w.Write(responseJson)
			return
		}
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var bodyMap = make(map[string]interface{})
		err = json.Unmarshal(body, &bodyMap)
		if err != nil {
			panic(err)
		}
		columnNamePK, err := getPrimaryColumnName(db, currTableName)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		delete(bodyMap, columnNamePK)
		typesForColumns, err := getTypesForColumns(db, currTableName)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		invalidField, err := findInvalidTypeField(bodyMap, typesForColumns)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			responseJson, _ := json.Marshal(SR{
				"error": fmt.Sprintf(`"field %s have invalid type"`, invalidField),
			})
			w.Write(responseJson)
			return
		}
		idCreated, err := createRow(db, currTableName, bodyMap)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		responseJson, _ := json.Marshal(SR{
			"response": SR{
				"id": idCreated,
			},
		})
		w.Write(responseJson)
		return
	}
	w.WriteHeader(http.StatusInternalServerError)
	return
}

func createRow(db *sql.DB, tableName string, bodyMap map[string]interface{}) (int, error) {
	columnNames := make([]string, 0, len(bodyMap))
	columnValues := make([]interface{}, 0, len(bodyMap))
	questionMark := make([]string, 0, len(bodyMap))
	for key, value := range bodyMap {
		columnNames = append(columnNames, key)
		columnValues = append(columnValues, value)
		questionMark = append(questionMark, "?")
	}
	result, err := db.Exec(fmt.Sprintf(`INSERT INTO %[1]s (%[2]s) VALUES(%[3]s);`,
		tableName,
		strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(fmt.Sprintf("%s", columnNames), "[", ""), "]", ""), " ", ","),
		strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(fmt.Sprintf("%s", questionMark), "[", ""), "]", ""), " ", ","),
	), columnValues...)
	if err != nil {
		return 0, err
	}
	lastInsertId, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}
	return int(lastInsertId), nil
}

func rowDetailHandler(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	tableNames, err := getTableList(db)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	reTableName := regexp.MustCompile(`(?P<tablename>\w+)`)
	matchStrings := reTableName.FindAllString(r.URL.Path, 2)
	if len(matchStrings) > 0 {
		currTableName := matchStrings[0]
		if !contains(tableNames, currTableName) {
			w.WriteHeader(http.StatusNotFound)
			responseJson, _ := json.Marshal(SR{
				"error": "unknown table",
			})
			w.Write(responseJson)
			return
		}
		currRowId, err := strconv.Atoi(matchStrings[1])
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		SRRow, err := getRowDetail(db, currTableName, currRowId)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if SRRow == nil {
			w.WriteHeader(http.StatusNotFound)
			responseJson, _ := json.Marshal(SR{
				"error": "record not found",
			})
			w.Write(responseJson)
			return
		}
		w.WriteHeader(http.StatusOK)
		responseJson, _ := json.Marshal(SR{
			"response": SR{
				"record": SRRow,
			},
		})
		w.Write(responseJson)
		return
	}
	w.WriteHeader(http.StatusInternalServerError)
	return
}

func mainHandler(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	rePut, _ := regexp.Compile(`/\w+/`)
	if r.URL.Path == "/" {
		tableListHandler(w, r, db)
	} else if r.Method == "GET" && strings.Count(r.URL.Path, "/") == 1 {
		tableDetailHandler(w, r, db)
	} else if r.Method == "GET" && strings.Count(r.URL.Path, "/") == 2 {
		rowDetailHandler(w, r, db)
	} else if r.Method == "PUT" && rePut.MatchString(r.URL.Path) {
		rowCreateHandler(w, r, db)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные
func NewDbExplorer(db *sql.DB) (http.Handler, error) {

	siteMux := http.NewServeMux()
	siteMux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		mainHandler(writer, request, db)
	})

	return siteMux, nil
}
