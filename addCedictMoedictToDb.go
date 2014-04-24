/*
This program adds cedict and moedict data to the database.
*/
package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/lib/pq"
	"github.com/jsoendermann/blueMandoTools/moedict"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"unicode/utf8"
)


func main() {
	if len(os.Args) != 3 {
		fmt.Println("Usage: addCedictMoedictToDb <cedict text file> <moedict json dump file>")
		os.Exit(-1)
	}

	fmt.Println("Creating database...")

	// create new database file and defer closing it
    dbname := os.Getenv("BMT_DB")
    dbuser := os.Getenv("BMT_USER")
    dbpw := os.Getenv("BMT_PW")

	db, err := sql.Open("postgres", "user=" + dbuser + " dbname=" + dbname + " password=" + dbpw + " host=localhost sslmode=disable")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

//    deleteOldTables(db)

	addCedictData(db)
	addMoedictData(db)
}

func deleteOldTables(db *sql.DB) {
    // FIXME this function needs to check if any tables exist first
    //exec(db, "drop schema public cascade;")

}

func addCedictData(db *sql.DB) {
	fmt.Println("Loading raw Cedict data...")

	// load raw data into memory
	data, err := ioutil.ReadFile(os.Args[1])
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("Creating Cedict DB")

	// SQL code to create the table
	exec(db, `CREATE TABLE cedict (
                    id        SERIAL PRIMARY KEY,
                    trad      VARCHAR(50) NOT NULL,      
                    simp      VARCHAR(50) NOT NULL,     
                    pinyin    VARCHAR(200) NOT NULL,     
                    english   VARCHAR(500) NOT NULL,
                    runecount INTEGER NOT NULL)`)

	// create new transaction to add the data to the databse
	tx, err := db.Begin()
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	// prepare insert statement and defer closing it
	stmt, err := tx.Prepare(pq.CopyIn("cedict", "trad", "simp", "pinyin", "english", "runecount"));
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	defer stmt.Close()

	fmt.Println("Adding data to database...")

	// split data into lines
	lines := strings.Split(string(data), "\n")

	// regexp used to parse the lines in the raw, downloaded text file
	var parseRegexp, _ = regexp.Compile(`(.*?) (.*?) \[(.*?)\] \/(.*)\/`)

	// loop through all the lines in the text file
	for _, line := range lines {
		// only parse non-empty lines and those that are not comments (start with '#')
		if line != "" && !(strings.HasPrefix(line, "#")) {
			// use compiled regexp to parse line
			parseResult := parseRegexp.FindStringSubmatch(line)

			runecount := utf8.RuneCountInString(parseResult[1])

			if runecount != utf8.RuneCountInString(parseResult[2]) {
				fmt.Printf("Runecount of %q and %q unequal\n", parseResult[1], parseResult[2])
				panic(err)
			}

            //fmt.Println(strings.Replace(parseResult[3], "u:", "v", -1));

			// execute insert statement for this row
			_, err = stmt.Exec(parseResult[1], // trad
				parseResult[2],                                 // simp
				strings.Replace(parseResult[3], "u:", "v", -1), // pinyin (replace "u:" by "v")
				strings.Replace(parseResult[4], "/", " / ", -1),
				runecount) // english (replace "/" by " / ")
			if err != nil {
				fmt.Println(err)
				panic(err)
			}
		}
	}

    // To flush data
    _, err = stmt.Exec()
    if err != nil {
	    fmt.Println(err)
		panic(err)

    }

    fmt.Println("Commiting transaction...")
	// commit transation
	tx.Commit()

	fmt.Println("Creating indices...")

	exec(db, "CREATE INDEX trad_index ON cedict USING btree (trad)")
	exec(db, "CREATE INDEX simp_index ON cedict USING btree (simp)")
}

func addMoedictData(db *sql.DB) {
	fmt.Println("Loading raw Moedict data...")

	// load raw data into memory
	data, err := ioutil.ReadFile(os.Args[2])
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("Parsing JSON...")

	entries := make([]*moedict.Entry, 0)
	json.Unmarshal(data, &entries)

	fmt.Println("Creating Moedict Tables...")

	// SQL code to create the tables
	// FIXME adjust varchar lengths
	exec(db, `CREATE TABLE md_entries (
                    id                         SERIAL PRIMARY KEY,
                    title                      VARCHAR(100) UNIQUE NOT NULL,      
                    radical                    VARCHAR(10),     
                    stroke_count               INTEGER NOT NULL,     
                    non_radical_stroke_count   INTEGER NOT NULL)`)

	exec(db, `CREATE TABLE md_heteronyms (
                    id          SERIAL PRIMARY KEY,
                    entry_id    INTEGER NOT NULL,
                    idx         INTEGER NOT NULL,
                    pinyin      VARCHAR(250),
                    bopomofo    VARCHAR(250),
                    bopomofo2   VARCHAR(250),
                    FOREIGN KEY (entry_id) REFERENCES md_entries(id))`)

	exec(db, `CREATE TABLE md_definitions (
                    id              SERIAL PRIMARY KEY,
                    heteronym_id    INTEGER NOT NULL,
                    idx             INTEGER NOT NULL,
                    def             VARCHAR(800),
                    quotes          VARCHAR(800),
                    examples        VARCHAR(800),
                    type            VARCHAR(10),
                    link            VARCHAR(100),
                    synonyms        VARCHAR(100),
                    antonyms        VARCHAR(100),
                    FOREIGN KEY (heteronym_id) REFERENCES md_heteronyms(id))`)

	fmt.Println("Inserting Moedict data...")

	entryId := 1
	heteronymId := 1
	definitionsId := 1
	for _, e := range entries {
		exec(db, fmt.Sprintf(`INSERT INTO md_entries(id, title, radical, stroke_count, non_radical_stroke_count) VALUES(%d, %s, %s, %d, %d)`,
			entryId,
			stringToSQLStatement(e.Title),
			stringToSQLStatement(e.Radical),
			e.Stroke_count,
			e.Non_radical_stroke_count))
                idx_h := 0
		for _, h := range e.Heteronyms {
			exec(db, fmt.Sprintf(`INSERT INTO md_heteronyms(id, entry_id, idx, pinyin, bopomofo, bopomofo2) VALUES(%d, %d, %d, %s, %s, %s)`,
				heteronymId,
				entryId,
				idx_h,
				stringToSQLStatement(h.Pinyin),
				stringToSQLStatement(h.Bopomofo),
				stringToSQLStatement(h.Bopomofo2)))
                        idx_d := 0
			for _, d := range h.Definitions {
                                links := strings.Join(d.Link, "|||")
				quotes := strings.Join(d.Quote, "|||")
				examples := strings.Join(d.Example, "|||")
				exec(db, fmt.Sprintf(`INSERT INTO md_definitions(id, heteronym_id, idx, def, quotes, examples, type, link, synonyms, antonyms) VALUES(%d, %d, %d, %s, %s, %s, %s, %s, %s, %s)`,
					definitionsId,
					heteronymId,
					idx_d,
					stringToSQLStatement(d.Def),
					stringToSQLStatement(quotes),
					stringToSQLStatement(examples),
					stringToSQLStatement(d.DefType),
					stringToSQLStatement(links),
					stringToSQLStatement(d.Synonyms),
					stringToSQLStatement(d.Antonyms)))
				definitionsId++
				idx_d++
			}
			heteronymId++
			idx_h++

		}
		entryId++

	}

    fmt.Println("Creating indices...")

	exec(db, "CREATE INDEX entries_title_index ON md_entries(title)")
	exec(db, "CREATE INDEX entries_id_index ON md_entries(id)")
	exec(db, "CREATE INDEX heteronyms_id_index ON md_heteronyms(id)")
	exec(db, "CREATE INDEX definitions_id_index ON md_definitions(id)")
}

func exec(db *sql.DB, statement string) sql.Result {
	//fmt.Println(statement)
	r, err := db.Exec(statement)
	if err != nil {
		panic(err)
	}
	return r
}

func stringToSQLStatement(s string) string {
	if s == "" {
		return "NULL"
	}
	return `'` + strings.Replace(s, "'", "''", -1) + `'`
}
