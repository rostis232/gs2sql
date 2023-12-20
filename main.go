package main

import (
	"context"
	"database/sql"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/mxmCherry/translit/uknational"
	"github.com/spf13/viper"
	"golang.org/x/oauth2/google"
	"golang.org/x/text/transform"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"

	_ "github.com/jackc/pgconn"
	_ "github.com/jackc/pgx/v4"
	_ "github.com/jackc/pgx/v4/stdlib"
)

type column struct {
	GSname    string `json:"gs_name"`
	DBname    string `json:"db_name"`
	ValueType string `json:"value_type"`
}

func createColumnList(titles []interface{}) ([]column, error) {
	var (
		startFound bool
		uuidFound  bool
	)
	set := []column{}
	for i, t := range titles {
		str, ok := t.(string)
		if !ok {
			return set, fmt.Errorf("error while converting title to string")
		}
		if str == "" {
			continue
		}
		setItem := column{GSname: str}
		if str == "start" {
			startFound = true
			setItem.DBname = "start"
			setItem.ValueType = "TIMESTAMPTZ"
			// setItem.ValueType = "VARCHAR(100)"
		} else if str == "_uuid" {
			uuidFound = true
			setItem.DBname = "uuid"
			setItem.ValueType = "UUID"
		} else {
			dbName, err := makeDBtitles(str)
			if err != nil {
				return set, err
			}
			setItem.DBname = dbName + fmt.Sprint(i)
			var setItemValueType string
			for {
				fmt.Printf("Визначте тип даних для поля %s:\n", str)
				fmt.Println("1 для INT")
				fmt.Println("2 для VARCHAR")
				fmt.Println("3 для FLOAT")
				fmt.Scan(&setItemValueType)
				if setItemValueType == "1" {
					setItem.ValueType = "INT"
					break
				} else if setItemValueType == "2" {
					setItem.ValueType = "VARCHAR"
					break
				} else if setItemValueType == "3" {
					setItem.ValueType = "FLOAT"
					break
				} else {
					fmt.Println("Помилка вводу. Повторіть")
				}
			}
		}
		set = append(set, setItem)
	}
	if startFound && uuidFound {
		return set, nil
	} else if !startFound && !uuidFound {
		return set, fmt.Errorf("start and uuid not found")
	} else if !startFound && uuidFound {
		return set, fmt.Errorf("start not found")
	} else {
		return set, fmt.Errorf("uuid not found")
	}
}

func makeDBtitles(cyrillic string) (string, error) {
	uk := uknational.ToLatin()
	s, _, err := transform.String(uk.Transformer(), strings.ToLower(cyrillic))
	if err != nil {
		return s, err
	}
	s = strings.ReplaceAll(s, " ", "_")
	s = strings.ReplaceAll(s, ",", "")
	s = strings.ReplaceAll(s, ".", "")
	s = strings.ReplaceAll(s, "\n", "")
	s = strings.ReplaceAll(s, ";", "")
	s = strings.ReplaceAll(s, ":", "")
	s = strings.ReplaceAll(s, "(", "")
	s = strings.ReplaceAll(s, ")", "")
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, "-", "_")
	if len(s) > 61 {
		s = string([]byte(s)[:60])
	}
	return s, nil
}

func iface2date(input interface{}) (time.Time, error) {
	tm := time.Time{}
	str, ok := input.(string)
	if !ok {
		return tm, fmt.Errorf("error while converting input to data")
	}
	tm, err := time.Parse("2006-01-02T15:04:05.000Z07:00", str)
	if err != nil {
		return tm, err
	}
	return tm, nil
}

func initConfig() error {
	viper.AddConfigPath("config")
	viper.SetConfigName("config")
	return viper.ReadInConfig()
}

func newPostgres(configDB string) (*sql.DB, error) {
	db, err := sql.Open("pgx", configDB)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func export(credentials string, spreadsheetId string, sheetName string) ([][]interface{}, error) {
	var result [][]interface{}
	ctx := context.Background()

	credBytes, err := b64.StdEncoding.DecodeString(credentials)
	if err != nil {
		return result, err
	}
	config, err := google.JWTConfigFromJSON(credBytes, "https://www.googleapis.com/auth/spreadsheets")
	if err != nil {
		return result, err
	}

	client := config.Client(ctx)

	srv, err := sheets.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return result, err
	}

	valueGetCall := srv.Spreadsheets.Values.Get(spreadsheetId, sheetName)

	values, err := valueGetCall.Do()
	if err != nil {
		return result, err
	}

	result = values.Values

	return result, nil
}

func createNewTableQuery(table string, data []column) (string, error) {
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n", table)
	for i, d := range data {
		str := fmt.Sprintf("%s %s", d.DBname, d.ValueType)
		if i < len(data)-1 {
			str += ",\n"
		} else {
			str += "\n"
		}
		query += str

	}
	query += ")"
	return query, nil
}

func createTable(db *sql.DB, query string) error {
	_, err := db.Exec(query)
	return err
}

func saveSetToDB(db *sql.DB, tableName string, sheetID string, listName string, set []column) error {
	jsonString, err := json.Marshal(set)
	if err != nil {
		return err
	}
	if err != nil {
		return err
	}
	query := "INSERT INTO sets (table_title, sheetID, listName, columns_set) VALUES ($1, $2, $3, $4)"

	_, err = db.Exec(query, tableName, sheetID, listName, string(jsonString))
	return err
}

func getSetFromDB(db *sql.DB, tableName string) (string, string, []column, error) {
	set := []column{}
	var (
		str      string
		sheetID  string
		listName string
	)
	query := "SELECT sheetID, listName, columns_set FROM sets WHERE table_title = $1"
	row := db.QueryRow(query, tableName)

	err := row.Scan(&sheetID, &listName, &str)
	if err != nil {
		return sheetID, listName, set, err
	}
	err = json.Unmarshal([]byte(str), &set)
	if err != nil {
		return sheetID, listName, set, err
	}
	return sheetID, listName, set, err
}

func creatingNewDB(db *sql.DB) {
	var (
		sheetID    string
		sheetTitle string
		tableName  string
	)

	fmt.Println("Введіть ІД таблиці:")
	fmt.Scan(&sheetID)
	fmt.Println("Введіть назву аркушу")
	fmt.Scan(&sheetTitle)
	fmt.Println("Введіть назву таблиці SQL")
	fmt.Scan(&tableName)
	data, err := export(viper.GetString("app.cred"), sheetID, sheetTitle+"!A1:XYZ1")
	if err != nil {
		log.Fatalln("error while config loading", err)
	}

	columns, err := createColumnList(data[0])
	if err != nil {
		log.Fatal(err)
	}

	query, err := createNewTableQuery(tableName, columns)
	if err != nil {
		log.Fatal(err)
	}

	err = createTable(db, query)
	if err != nil {
		log.Fatal(err)
	}

	err = saveSetToDB(db, tableName, sheetID, sheetTitle, columns)
	if err != nil {
		log.Fatal(err)
	}
}

func importToExistingDB(db *sql.DB) {
	var tbl string
	fmt.Println("Введіть назву таблиці")
	fmt.Scan(&tbl)
	sheetID, sheetTitle, set, err := getSetFromDB(db, tbl)
	if err != nil {
		log.Fatal(err)
	}

	data, err := export(viper.GetString("app.cred"), sheetID, sheetTitle+"!A:XYZ")
	if err != nil {
		log.Fatalln("error while config loading", err)
	}

	titlesMap := makeTitlesMap(set, data[0])

	for i := range data {
		if i == 0 {
			continue
		} else {
			query := createInsertQuery(tbl, set, titlesMap, data[i])
			_, err := db.Exec(query)
			if err != nil {
				fmt.Println(err)
			}
			// err := createInsertQuery(db, tbl, set, titlesMap, data[i])
			// if err != nil {
			// 	fmt.Println(err)
			// }
		}
	}
}

func makeTitlesMap(set []column, gsTitles []interface{}) map[string]int {
	titlesMap := make(map[string]int)
	for _, s := range set {
		for j, title := range gsTitles {
			str, ok := title.(string)
			if !ok {
				log.Fatal("makeDBtitlesMap error converting interface to string")
			}
			if s.GSname == str {
				titlesMap[s.GSname] = j
				break
			}
		}
	}
	return titlesMap
}

func createInsertQuery(table string, set []column, titles map[string]int, data []interface{}) string {
	query := "INSERT INTO " + table + " ("
	for i, s := range set {
		query += s.DBname
		if i < len(set)-1 {
			query += ", "
		}
	}
	query += ") VALUES ("
	for i, s := range set {
		for j, d := range data {
			if titles[s.GSname] == j {
				str, ok := d.(string)
				if !ok {
					log.Fatal("createInsertQuery error while converting interface to string")
				}
				if str == "" && s.ValueType == "INT" {
					str = "0"
				}
				str = strings.ReplaceAll(str, "`", "")
				str = strings.ReplaceAll(str, "'", "")
				str = strings.ReplaceAll(str, "\"", "")
				query += "'" + str + "'"
				break
			}
		}
		if i < len(set)-1 {
			query += ", "
		}
	}
	query += ")"
	return query
}

func deleteData(db *sql.DB, table string) error {
	query := "DELETE FROM " + table
	_, err := db.Exec(query)
	return err
}

// func createInsertQuery(db *sql.DB, table string, set []column, titles map[string]int, data []interface{}) error {
// 	query := "INSERT INTO " + table + " ("
// 	for i, s := range set {
// 		query += s.DBname
// 		if i < len(set)-1 {
// 			query += ", "
// 		}
// 	}
// 	query += ") VALUES ("
// 	for i := range set {
// 		query += fmt.Sprintf("$%d", i+1)
// 		if i < len(set)-1 {
// 			query += ", "
// 		}
// 	}
// 	query += ")"

// 	newData := []interface{}{}
// 	for _, s := range set {
// 		for j, d := range data {
// 			if titles[s.GSname] == j {
// 				str, ok := d.(string)
// 				if !ok {
// 					log.Fatalln("createInsertQuery error")
// 				}
// 				if s.ValueType == "INTEGER" && str == "" {
// 					newData = append(newData, interface{}(0))
// 				} else {
// 					newData = append(newData, &d)
// 				}
				
// 				break
// 			}
// 		}
// 	}
// 	fmt.Println(query)

// 	_, err := db.Exec(query, newData...)
// 	return err
// }

func main() {
	//ініціювали конфіг
	if err := initConfig(); err != nil {
		log.Fatalln("error while config loading", err)
	}

	//конектимось до БД
	db, err := newPostgres(viper.GetString("db.postgres"))
	if err != nil {
		log.Fatalln("error while config loading", err)
	}

	for {
		var ch string
		fmt.Println("Що зробити?")
		fmt.Println("1 - створити нову базу даних")
		fmt.Println("2 - заповнити стоврену таблицю даними")
		fmt.Println("3 - очистити таблицю")
		fmt.Scan(&ch)
		if ch == "1" {
			creatingNewDB(db)
		} else if ch == "2" {
			importToExistingDB(db)

		} else if ch == "3" {
			var tbl string
			fmt.Println("Введіть назву таблиці")
			fmt.Scan(&tbl)
			deleteData(db, tbl)
		} else {
			fmt.Println("Неправильне введення")
			continue
		}
	}
}
