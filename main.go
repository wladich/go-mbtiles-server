// go-mbtiles-server project main.go
package main

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	dataDir = "/home/w/projects/mbtiles_layers/local_display"
)

type Layer struct {
	conn     *sql.DB
	tileStmt *sql.Stmt
}

func newLayer(filename string) (layer *Layer, err error) {
	layer = new(Layer)
	layer.conn, err = sql.Open("sqlite3", filename)
	if err != nil {
		return nil, err
	}
	layer.tileStmt, err = layer.conn.Prepare("SELECT tile_data FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?")
	return layer, err
}

func (layer *Layer) tile(x, y, z int) ([]byte, error) {
	rows, err := layer.tileStmt.Query(z, x, y)
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	if rows.Next() {
		var buf []byte
		rows.Scan(&buf)
		return buf, nil
	} else {
		err = rows.Err()
		return nil, err
	}

}

var layers = make(map[string]*Layer)

func loadLayers() {
	var err error
	mbtiles, _ := filepath.Glob(filepath.Join(dataDir, "*.mbtiles"))
	log.Printf("Found %d mbtiles files", len(mbtiles))
	for _, filename := range mbtiles {
		name := filepath.Base(filename)
		name = strings.TrimSuffix(name, ".mbtiles")
		layers[name], err = newLayer(filename)
		if err != nil {
			log.Printf("Error opening mbtiles file \"%s\": %s", filename, err)
		} else {
			log.Printf("Loaded file \"%s\" as \"%s\"", filename, name)
		}
	}
}

func tileResponse(resp http.ResponseWriter, req *http.Request) {
	url := req.URL.Path
	urlFields := strings.Split(url, "/")
	if len(urlFields) != 5 {
		http.NotFound(resp, req)
		return
	}
	layer, ok := layers[urlFields[1]]
	if !ok {
		http.NotFound(resp, req)
		return
	}
	z, err := strconv.Atoi(urlFields[2])
	if err != nil {
		http.NotFound(resp, req)
		return
	}
	x, err := strconv.Atoi(urlFields[3])
	if err != nil {
		http.NotFound(resp, req)
		return
	}
	y, err := strconv.Atoi(urlFields[4])
	if err != nil {
		http.NotFound(resp, req)
		return
	}
	data, err := layer.tile(x, y, z)
	if err != nil {
		log.Printf("Error getting tile from layer \"%s\" z=%d x=%d y=%d: %v", urlFields[1], z, x, y, err)
		http.Error(resp, "", 500)
		return
	}
	if data == nil {
		fmt.Println("Tile not found")
		http.NotFound(resp, req)
		return
	} else {
		resp.Header().Add("Content-Type", "image/png")
		resp.Write(data)
	}
}

func main() {
	loadLayers()
	http.HandleFunc("/", tileResponse)
	log.Fatal(http.ListenAndServe(":8080", nil))

}
