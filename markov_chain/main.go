package main

import (
	crand "crypto/rand"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"strings"

	"github.com/neo4j/neo4j-go-driver/neo4j"
	"github.com/o-sk/neo4j-sample/config"
	"github.com/pkg/errors"
)

type token struct {
	surface  string
	features string
}

func main() {
	seed, _ := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
	rand.Seed(seed.Int64())

	cfg := config.Load("config.yml")
	driver, err := neo4j.NewDriver(cfg.Neo4j.URI, neo4j.BasicAuth(cfg.Neo4j.Username, cfg.Neo4j.Password, ""))
	if err != nil {
		fmt.Print(err)
		return
	}
	defer driver.Close()

	session, err := driver.Session(neo4j.AccessModeRead)
	if err != nil {
		fmt.Print(err)
		return
	}
	defer session.Close()

	tokens := make([]*token, 0)
	first, second, err := fetchStartTokens(session)
	if err != nil {
		fmt.Print(err)
		return
	}
	tokens = append(tokens, first, second)

	l := 2
	for hasNext(session, tokens[l-2], tokens[l-1]) {
		t, err := fetchNextToken(session, tokens[l-2], tokens[l-1])
		if err != nil {
			fmt.Print(err)
			return
		}
		tokens = append(tokens, t)
		l++
	}

	surfaces := make([]string, len(tokens))
	for _, t := range tokens {
		surfaces = append(surfaces, t.surface)
	}

	fmt.Printf("%s\n", strings.Join(surfaces, ""))
}

func fetchStartTokens(session neo4j.Session) (*token, *token, error) {
	result, err := session.Run(
		`MATCH (a:Dummy) -[]-> (n1:Token) -[]-> (n2:Token)
		RETURN n1.surface, n1.features, n2.surface, n2.features`,
		map[string]interface{}{})
	if err != nil {
		return nil, nil, err
	}

	type row struct {
		n1Surface, n1Features, n2Surface, n2Features string
	}
	rows := make([]*row, 0)
	for result.Next() {
		record := result.Record()
		r := &row{
			n1Surface:  record.GetByIndex(0).(string),
			n1Features: record.GetByIndex(1).(string),
			n2Surface:  record.GetByIndex(2).(string),
			n2Features: record.GetByIndex(3).(string),
		}
		rows = append(rows, r)
	}

	if len(rows) == 0 {
		return nil, nil, errors.New("Not Found")
	}

	r := rows[rand.Intn(len(rows))]
	first := &token{
		surface:  r.n1Surface,
		features: r.n1Features,
	}
	second := &token{
		surface:  r.n2Surface,
		features: r.n2Features,
	}
	return first, second, nil
}

func fetchNextToken(session neo4j.Session, n1, n2 *token) (*token, error) {
	result, err := session.Run(
		`MATCH (n1{surface: $n1Surface, features: $n1Features})-[r1:Text]->
		(n2{surface: $n2Surface, features: $n2Features})-[r2:Text]->(n3:Token)
		WHERE r1.message = r2. message
		RETURN n3.surface, n3.features, count(n3)`,
		map[string]interface{}{
			"n1Surface":  n1.surface,
			"n1Features": n1.features,
			"n2Surface":  n2.surface,
			"n2Features": n2.features,
		})
	if err != nil {
		return nil, err
	}

	type row struct {
		surface, features string
		count             int64
	}
	tokens := make([]*token, 0)
	for result.Next() {
		record := result.Record()
		surface := record.GetByIndex(0).(string)
		features := record.GetByIndex(1).(string)
		count := record.GetByIndex(2).(int64)
		for i := 0; int64(i) < count; i++ {
			tokens = append(tokens, &token{surface: surface, features: features})
		}
	}

	if len(tokens) == 0 {
		return nil, errors.New("Not Found")
	}

	t := tokens[rand.Intn(len(tokens))]
	return t, nil
}

func hasNext(session neo4j.Session, n1, n2 *token) bool {
	result, err := session.Run(
		`MATCH (n1{surface: $n1Surface, features: $n1Features})-[r1:Text]->
		(n2{surface: $n2Surface, features: $n2Features})-[r2:Text]->(n3:Dummy)
		WHERE r1.message = r2. message
		RETURN n3`,
		map[string]interface{}{
			"n1Surface":  n1.surface,
			"n1Features": n1.features,
			"n2Surface":  n2.surface,
			"n2Features": n2.features,
		})
	if err != nil {
		return false
	}

	if result.Next() {
		return false
	}

	return true
}
