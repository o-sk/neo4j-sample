package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/ikawaha/kagome/tokenizer"
	"github.com/neo4j/neo4j-go-driver/neo4j"
	"github.com/o-sk/neo4j-sample/config"
)

func main() {
	cfg := config.Load("config.yml")
	driver, err := neo4j.NewDriver(cfg.Neo4j.URI, neo4j.BasicAuth(cfg.Neo4j.Username, cfg.Neo4j.Password, ""))
	if err != nil {
		fmt.Print(err)
		return
	}
	defer driver.Close()

	dic := tokenizer.SysDicSimple()
	t := tokenizer.NewWithDic(dic)

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		tokens := t.Analyze(line, tokenizer.Normal)
		for i, tok := range tokens {
			err := createToken(driver, tok)
			if err != nil {
				fmt.Print(err)
			}
			if i > 0 {
				err = createRelation(driver, tokens[i-1], tok, line)
				if err != nil {
					fmt.Print(err)
				}
			}
		}
	}
	if scanner.Err() != nil {
		fmt.Print(scanner.Err())
	}
}

func createToken(driver neo4j.Driver, tok tokenizer.Token) error {
	if tok.Class == tokenizer.DUMMY {
		return createDummyToken(driver, tok)
	} else {
		return createNormalToken(driver, tok)
	}
}

func createDummyToken(driver neo4j.Driver, tok tokenizer.Token) error {
	session, err := driver.Session(neo4j.AccessModeWrite)
	if err != nil {
		return err
	}
	defer session.Close()
	_, err = session.WriteTransaction(func(transaction neo4j.Transaction) (interface{}, error) {
		result, err := transaction.Run(
			"MERGE (t:Dummy { surface: $surface }) RETURN t",
			map[string]interface{}{"surface": tok.Surface})
		if err != nil {
			return nil, err
		}

		if result.Next() {
			return "", nil
		}

		return nil, result.Err()
	})
	return err
}

func createNormalToken(driver neo4j.Driver, tok tokenizer.Token) error {
	session, err := driver.Session(neo4j.AccessModeWrite)
	if err != nil {
		return err
	}
	defer session.Close()
	_, err = session.WriteTransaction(func(transaction neo4j.Transaction) (interface{}, error) {
		result, err := transaction.Run(
			"MERGE (t:Token { surface: $surface, features: $features }) RETURN t",
			map[string]interface{}{"surface": tok.Surface, "features": strings.Join(tok.Features(), ",")})
		if err != nil {
			return nil, err
		}

		if result.Next() {
			return "", nil
		}

		return nil, result.Err()
	})
	return err
}

func createRelation(driver neo4j.Driver, from, to tokenizer.Token, message string) error {
	if from.Class == tokenizer.DUMMY {
		return createBeginRelation(driver, from, to, message)
	} else if to.Class == tokenizer.DUMMY {
		return createEndRelation(driver, from, to, message)
	} else {
		return createNormalRelation(driver, from, to, message)
	}
}

func createBeginRelation(driver neo4j.Driver, from, to tokenizer.Token, message string) error {
	session, err := driver.Session(neo4j.AccessModeWrite)
	if err != nil {
		return err
	}
	defer session.Close()
	_, err = session.WriteTransaction(func(transaction neo4j.Transaction) (interface{}, error) {
		result, err := transaction.Run(
			`MATCH (from:Dummy), (to:Token)
WHERE from.surface = $from_surface AND to.surface = $to_surface AND to.features = $to_features
CREATE (from)-[r:Text {message: $message}]->(to)
RETURN r`,
			map[string]interface{}{
				"from_surface": from.Surface,
				"to_surface":   to.Surface,
				"to_features":  strings.Join(to.Features(), ","),
				"message":      message,
			})
		if err != nil {
			return nil, err
		}

		if result.Next() {
			return "", nil
		}

		return nil, result.Err()
	})
	return err
}

func createEndRelation(driver neo4j.Driver, from, to tokenizer.Token, message string) error {
	session, err := driver.Session(neo4j.AccessModeWrite)
	if err != nil {
		return err
	}
	defer session.Close()
	_, err = session.WriteTransaction(func(transaction neo4j.Transaction) (interface{}, error) {
		result, err := transaction.Run(
			`MATCH (from:Token), (to:Dummy)
WHERE from.surface = $from_surface AND from.features = $from_features
  AND to.surface = $to_surface
CREATE (from)-[r:Text {message: $message}]->(to)
RETURN r`,
			map[string]interface{}{
				"from_surface":  from.Surface,
				"from_features": strings.Join(from.Features(), ","),
				"to_surface":    to.Surface,
				"message":       message,
			})
		if err != nil {
			return nil, err
		}

		if result.Next() {
			return "", nil
		}

		return nil, result.Err()
	})
	return err
}

func createNormalRelation(driver neo4j.Driver, from, to tokenizer.Token, message string) error {
	session, err := driver.Session(neo4j.AccessModeWrite)
	if err != nil {
		return err
	}
	defer session.Close()
	_, err = session.WriteTransaction(func(transaction neo4j.Transaction) (interface{}, error) {
		result, err := transaction.Run(
			`MATCH (from:Token), (to:Token)
WHERE from.surface = $from_surface AND from.features = $from_features
  AND to.surface = $to_surface AND to.features = $to_features
CREATE (from)-[r:Text {message: $message}]->(to)
RETURN r`,
			map[string]interface{}{
				"from_surface":  from.Surface,
				"from_features": strings.Join(from.Features(), ","),
				"to_surface":    to.Surface,
				"to_features":   strings.Join(to.Features(), ","),
				"message":       message,
			})
		if err != nil {
			return nil, err
		}

		if result.Next() {
			return "", nil
		}

		return nil, result.Err()
	})
	return err
}
