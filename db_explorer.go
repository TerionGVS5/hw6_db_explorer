package main

import (
	"database/sql"
	"encoding/json"
	"net/http"
)

type SR map[string]interface{}

func tableListHandler(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	var tableNames []string
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		tableNames = append(tableNames, tableName)

	}
	w.WriteHeader(http.StatusOK)
	responseJson, _ := json.Marshal(SR{
		"response": SR{
			"tables": tableNames,
		},
	})
	w.Write(responseJson)
}

func mainHandler(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	if r.URL.Path == "/" {
		tableListHandler(w, r, db)
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
