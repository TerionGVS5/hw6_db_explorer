package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
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

func mainHandler(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	reTableDetail := regexp.MustCompile(`/[\w?=&]+`)
	if r.URL.Path == "/" {
		tableListHandler(w, r, db)
	} else if r.Method == "GET" && reTableDetail.MatchString(r.URL.Path) && strings.Count(r.URL.Path, "/") == 1 {
		tableDetailHandler(w, r, db)
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
