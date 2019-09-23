package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	minhashlsh "github.com/ekzhu/minhash-lsh"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"google.golang.org/appengine"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

var (
	threshold = 0.1
)

func indexing(db *sql.DB) (lsh *minhashlsh.MinhashLSH, minhashSize, minhashSeed int) {
	sqlPredicates := `count != empty_count
					AND (
						distinct_count >= 50
						OR (
							distinct_count >= 10
							AND distinct_count::float / (count - empty_count)::float >= 0.9
						)
					)`
	log.Print("Counting indexable column sketches...")
	var count int
	err := db.QueryRow(`SELECT count(*) as count
			FROM findopendata.column_sketches 
			WHERE ` + sqlPredicates).Scan(&count)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Indexing started, scanning %d column sketches...", count)
	rows, err := db.Query(`SELECT id, minhash, seed 
						FROM findopendata.column_sketches
						WHERE ` + sqlPredicates)
	if err != nil {
		log.Fatal(err)
	}
	minhashSize, minhashSeed = -1, -1
	var id string
	var minhash []int64
	var seed int
	for rows.Next() {
		if err := rows.Scan(&id, pq.Array(&minhash), &seed); err != nil {
			log.Fatal(err)
		}
		if minhashSize == -1 {
			minhashSize = len(minhash)
		} else if minhashSize != len(minhash) {
			log.Fatalf("Incorrect minhash size %v encountered, expecting %v", len(minhash), minhashSize)
		}
		if minhashSeed == -1 {
			minhashSeed = seed
		} else if minhashSeed != seed {
			log.Fatalf("Incorrect minhash seed %v encountered, expecting %v", seed, minhashSeed)
		}
		if lsh == nil {
			lsh = minhashlsh.NewMinhashLSH(minhashSize, threshold, count)
		}
		minhashUnsigned := make([]uint64, len(minhash))
		for i := range minhash {
			minhashUnsigned[i] = uint64(minhash[i])
		}
		lsh.Add(id, minhashUnsigned)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	log.Print("Building LSH index...")
	lsh.Index()
	log.Printf("Finished indexing %d minhashes", count)
	return
}

type request struct {
	Seed    int      `json:"seed"`
	Minhash []uint64 `json:"minhash"`
}

type response []string

func main() {
	// Initiate Postgres connection.
	// Obtain the connection parameters from environment variables.
	db, err := sql.Open("postgres", "")
	if err != nil {
		log.Fatalf("Could not open db: %v", err)
	}
	// Build Minhash LSH index.
	lsh, minhashSize, minhashSeed := indexing(db)
	// Close database connection.
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	// Configure gin server.
	var router *gin.Engine
	if os.Getenv("MODE") == "release" {
		gin.SetMode(gin.ReleaseMode)
		router = gin.New()
		router.Use(gin.ErrorLoggerT(gin.ErrorTypePublic))
		router.Use(gin.Recovery())
		router.Use(cors.Default())
	} else {
		router = gin.Default()
	}
	router.POST("/lsh/query", func(c *gin.Context) {
		body, err := ioutil.ReadAll(io.LimitReader(c.Request.Body, 1024*1024))
		if err != nil {
			c.Error(fmt.Errorf("Error reading request")).SetType(gin.ErrorTypePublic)
			c.AbortWithError(http.StatusBadRequest, err)
			return
		}
		var req request
		if err := json.Unmarshal(body, &req); err != nil {
			c.Error(fmt.Errorf("Error reading JSON request body")).SetType(gin.ErrorTypePublic)
			c.AbortWithError(http.StatusUnprocessableEntity, err)
			return
		}
		if len(req.Minhash) != minhashSize {
			err := fmt.Errorf("Incorrect minhash size, expecting %v", minhashSize)
			c.Error(err).SetType(gin.ErrorTypePublic)
			c.AbortWithError(http.StatusBadRequest, err)
			return
		}
		if req.Seed != minhashSeed {
			err := fmt.Errorf("Incorrect minhash seed, expecting %v", minhashSeed)
			c.Error(err).SetType(gin.ErrorTypePublic)
			c.AbortWithError(http.StatusBadRequest, err)
			return
		}
		results := lsh.Query(req.Minhash)
		resp := make(response, len(results))
		for i := range results {
			resp[i] = results[i].(string)
		}
		c.JSON(http.StatusOK, resp)
		return
	})

	http.Handle("/", router)
	if os.Getenv("MODE") == "release" {
		appengine.Main()
	} else {
		log.Fatal(http.ListenAndServe(":8081", nil))
	}
}
