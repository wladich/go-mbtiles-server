package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	html = `
<!DOCTYPE html>
<html>
    <header>
        <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no" />
        <link rel="stylesheet" href="http://cdn.leafletjs.com/leaflet-0.7/leaflet.css" />
        <script src="http://cdn.leafletjs.com/leaflet-0.7/leaflet.js"></script>
        <script src="https://rawgithub.com/mlevans/leaflet-hash/master/leaflet-hash.js"></script>
        <style>
            body, html, #map {
             height: 100%%;
            }
        </style>

        <script>
            var layers = [%s];

            function setUpMap(){
                 map = new L.Map('map', {fadeAnimation: false});
                 baseMaps = {};
                 for (i=0; i < layers.length; i++) {
                    name = layers[i];
                    url = "/" + name+ "/{z}/{x}/{y}";
                    layer  = new L.TileLayer(url, {tms: true});
                    baseMaps[name] = layer;
                    if (i==0) {
                        layer.addTo(map);
                    }
                 }
                 L.control.layers(baseMaps, {}, {collapsed: false}).addTo(map);
                 map.setView([55, 36], 9);
                 var hash = new L.Hash(map);
            }

            window.onload = setUpMap;
        </script>
    </header>
    <body style="margin: 0">
        <div id="map"></div>

    </body>
</html>
`
)

type Layer struct {
	conn           *sql.DB
	tileStmt       *sql.Stmt
	activeRequests sync.WaitGroup
	mtime          time.Time
	size           int64
	valid          bool
}

func newLayer(filename string) (layer *Layer, err error) {
	layer = new(Layer)
	layer.conn, err = sql.Open("sqlite3", filename)
	if err != nil {
		layer.valid = false
		return
	}
	layer.conn.SetMaxOpenConns(5)
	layer.conn.SetMaxIdleConns(5)
	layer.tileStmt, err = layer.conn.Prepare("SELECT tile_data FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?")
	if err != nil {
		layer.conn.Close()
		layer.valid = false
		return
	}
	layer.activeRequests.Add(1)
	layer.valid = true
	go func() {
		layer.activeRequests.Wait()
		layer.tileStmt.Close()
		layer.conn.Close()
		log.Printf("Layer %s disposed", filename)
	}()
	return
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
var startingRequests sync.RWMutex

func updateLayers(dataDir *string) {
	for {
		files, _ := filepath.Glob(filepath.Join(*dataDir, "*.mbtiles"))
		seenLayers := make(map[string]bool)
		for _, path := range files {
			fi, err := os.Stat(path)
			if err != nil || fi.IsDir() {
				continue
			}
			mtime, size := fi.ModTime(), fi.Size()
			name := filepath.Base(path)
			name = strings.TrimSuffix(name, ".mbtiles")
			seenLayers[name] = true
			oldLayer, layerExists := layers[name]
			if !layerExists || oldLayer.mtime != mtime || oldLayer.size != size {
				layer, err := newLayer(path)
				layer.mtime = mtime
				layer.size = size
				if err != nil {
					log.Printf("Error opening mbtiles file \"%s\": %s", path, err)
				}
				layers[name] = layer
				if layerExists && oldLayer.valid {
					startingRequests.Lock()
					oldLayer.activeRequests.Add(-1)
					startingRequests.Unlock()
					log.Printf("Updated file \"%s\" as \"%s\"", path, name)
				} else {
					log.Printf("Loaded file \"%s\" as \"%s\"", path, name)
				}
			}
		}
		for name, layer := range layers {
			if _, ok := seenLayers[name]; !ok && layer.valid {
				startingRequests.Lock()
				delete(layers, name)
				layer.activeRequests.Add(-1)
				startingRequests.Unlock()
				log.Printf("Layer \"%s\" removed", name)
			}
		}
		time.Sleep(time.Second)
	}

}

func tileResponse(resp http.ResponseWriter, req *http.Request) {
	resp.Header().Add("Access-Control-Allow-Origin", "*")
	url := req.URL.Path
	urlFields := strings.Split(url, "/")
	if len(urlFields) != 5 {
		http.NotFound(resp, req)
		return
	}
	startingRequests.RLock()
	layer, ok := layers[urlFields[1]]
	if !ok {
		startingRequests.RUnlock()
		http.NotFound(resp, req)
		return
	} else {
		layer.activeRequests.Add(1)
		defer func() {
			layer.activeRequests.Add(-1)
		}()
		startingRequests.RUnlock()
	}
	if layer.tileStmt == nil {
		http.Error(resp, "layer invalid", 500)
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
		//fmt.Println("Tile not found")
		http.NotFound(resp, req)
		return
	} else {
		resp.Header().Add("Content-Type", "image/png")
		resp.Write(data)
	}
}

func viewer(resp http.ResponseWriter, req *http.Request) {
	layersNames := make([]string, len(layers))
	i := 0
	for name := range layers {
		layersNames[i] = "\"" + name + "\""
		i++
	}
	fmt.Fprintf(resp, html, strings.Join(layersNames, ","))
}

func route(resp http.ResponseWriter, req *http.Request) {
	if req.URL.Path == "/" {
		viewer(resp, req)
	} else {
		tileResponse(resp, req)
	}

}

func main() {
	port := flag.Int("port", 8080, "port to listen")
	host := flag.String("host", "127.0.0.1", "address to bind to")
	dataDir := flag.String("path", ".", "where to look for *.mbtiles files")
	flag.Parse()
	go updateLayers(dataDir)
	http.HandleFunc("/", route)
	log.Fatal(http.ListenAndServe(fmt.Sprintf("%s:%d", *host, *port), nil))

}
