// Copyright 2026 tamnd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build neo4j

// Package neo4j is the YCSB adapter for Neo4j over Bolt.
//
// Neo4j is the one engine in this comparison that cannot be embedded, so
// every operation here crosses a socket and pays a protocol round trip
// that the in process engines do not. That is not a flaw in the harness,
// it is what running Neo4j costs, and a comparison that hid it would be
// describing a product nobody can buy. Point the adapter at a server on
// the same machine so the hop is loopback and not a network.
package neo4j

import (
	"context"
	"fmt"
	"strings"

	"github.com/magiconair/properties"
	neo "github.com/neo4j/neo4j-go-driver/v6/neo4j"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j/config"

	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// Neo4j properties.
const (
	neoURI      = "neo4j.uri"
	neoUsername = "neo4j.username"
	neoPassword = "neo4j.password"
	neoDatabase = "neo4j.database"
	neoPoolSize = "neo4j.maxconnpoolsize"
	neoFetchAll = "neo4j.fetchall"
)

type neoCreator struct{}

type neoDB struct {
	p       *properties.Properties
	verbose bool

	driver   neo.Driver
	database string
	fetchAll bool

	fieldCount int64
	table      string
}

type ctxKey struct{}

func (c neoCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	d := new(neoDB)
	d.p = p
	d.verbose = p.GetBool(prop.Verbose, prop.VerboseDefault)
	d.fieldCount = p.GetInt64(prop.FieldCount, prop.FieldCountDefault)
	d.table = p.GetString(prop.TableName, prop.TableNameDefault)
	d.database = p.GetString(neoDatabase, "neo4j")
	d.fetchAll = p.GetBool(neoFetchAll, true)

	uri := p.GetString(neoURI, "bolt://127.0.0.1:7687")
	user := p.GetString(neoUsername, "neo4j")
	pass := p.GetString(neoPassword, "password")

	threadCount := int(p.GetInt64(prop.ThreadCount, prop.ThreadCountDefault))
	poolSize := p.GetInt(neoPoolSize, 0)
	if poolSize <= 0 {
		// One connection per worker plus headroom for the setup session.
		// Leaving this at the driver default would make the concurrency
		// sweep measure pool contention rather than the server.
		poolSize = threadCount + 4
	}

	driver, err := neo.NewDriver(uri, neo.BasicAuth(user, pass, ""), func(c *config.Config) {
		c.MaxConnectionPoolSize = poolSize
		if d.fetchAll {
			// Every YCSB result set is small and already bounded by a
			// LIMIT, so batching the pull only buys extra round trips.
			c.FetchSize = neo.FetchAll
		}
	})
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	if err := driver.VerifyConnectivity(ctx); err != nil {
		driver.Close(ctx)
		return nil, fmt.Errorf("neo4j: cannot reach %s: %w", uri, err)
	}
	d.driver = driver

	if err := d.createSchema(ctx); err != nil {
		driver.Close(ctx)
		return nil, err
	}
	return d, nil
}

// createSchema drops the previous data and puts the key index back.
func (db *neoDB) createSchema(ctx context.Context) error {
	s := db.driver.NewSession(ctx, neo.SessionConfig{DatabaseName: db.database})
	defer s.Close(ctx)

	if db.p.GetBool(prop.DropData, prop.DropDataDefault) {
		// CALL IN TRANSACTIONS rather than one DETACH DELETE, because a
		// single transaction over a full load holds the whole delete set
		// in the heap and falls over on anything but a toy record count.
		q := fmt.Sprintf("MATCH (u:%s) CALL { WITH u DETACH DELETE u } IN TRANSACTIONS OF 10000 ROWS", db.table)
		if _, err := s.Run(ctx, q, nil); err != nil {
			return fmt.Errorf("neo4j: drop data: %w", err)
		}
	}

	// The uniqueness constraint is what gives Neo4j a backing index on
	// the key. Without it every read is a label scan and the comparison
	// is against an engine nobody would deploy.
	q := fmt.Sprintf("CREATE CONSTRAINT ycsb_key_unique IF NOT EXISTS FOR (u:%s) REQUIRE u.ycsb_key IS UNIQUE", db.table)
	if _, err := s.Run(ctx, q, nil); err != nil {
		return fmt.Errorf("neo4j: create constraint: %w", err)
	}
	return nil
}

func (db *neoDB) Close() error {
	return db.driver.Close(context.Background())
}

// InitThread gives each worker its own session. A session is a cursor
// over one connection and is not safe to share between goroutines.
func (db *neoDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	s := db.driver.NewSession(ctx, neo.SessionConfig{DatabaseName: db.database})
	return context.WithValue(ctx, ctxKey{}, s)
}

func (db *neoDB) CleanupThread(ctx context.Context) {
	if s := sessionOf(ctx); s != nil {
		s.Close(ctx)
	}
}

func sessionOf(ctx context.Context) neo.Session {
	s, _ := ctx.Value(ctxKey{}).(neo.Session)
	return s
}

// fieldNames returns the fields to project, defaulting to all of them.
func (db *neoDB) fieldNames(fields []string) []string {
	if len(fields) > 0 {
		return fields
	}
	all := make([]string, 0, db.fieldCount)
	for i := int64(0); i < db.fieldCount; i++ {
		all = append(all, fmt.Sprintf("field%d", i))
	}
	return all
}

// params turns the YCSB value map into Bolt parameters. Values arrive as
// bytes and go out as strings, because Bolt would otherwise ship them as
// byte arrays and the read side would come back with a different type
// than it went in with.
func params(key string, values map[string][]byte) map[string]any {
	m := make(map[string]any, len(values)+1)
	m["k"] = key
	for _, p := range util.NewFieldPairs(values) {
		m[p.Field] = string(p.Value)
	}
	return m
}

// collect drains a result into field maps. Only string values are kept,
// which is every column this schema has.
func collect(ctx context.Context, res neo.Result, capacity int) ([]map[string][]byte, error) {
	out := make([]map[string][]byte, 0, capacity)
	for res.Next(ctx) {
		rec := res.Record()
		m := make(map[string][]byte, len(rec.Keys))
		for i, k := range rec.Keys {
			if s, ok := rec.Values[i].(string); ok {
				m[k] = []byte(s)
			}
		}
		out = append(out, m)
	}
	if err := res.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// returnClause builds "u.field0 AS field0, u.field1 AS field1".
func returnClause(fields []string) string {
	var b strings.Builder
	for i, f := range fields {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "u.%s AS %s", f, f)
	}
	return b.String()
}

func (db *neoDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	cols := db.fieldNames(fields)
	q := fmt.Sprintf("MATCH (u:%s {ycsb_key: $k}) RETURN %s", table, returnClause(cols))

	res, err := sessionOf(ctx).Run(ctx, q, map[string]any{"k": key})
	if err != nil {
		return nil, err
	}
	rows, err := collect(ctx, res, 1)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return rows[0], nil
}

func (db *neoDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	cols := db.fieldNames(fields)
	// A YCSB scan is the next count records in key order, so ORDER BY is
	// part of the operation and not a decoration. Without it the server
	// is free to return whatever the index walk happened to touch.
	q := fmt.Sprintf("MATCH (u:%s) WHERE u.ycsb_key >= $k RETURN %s ORDER BY u.ycsb_key LIMIT $n",
		table, returnClause(cols))

	res, err := sessionOf(ctx).Run(ctx, q, map[string]any{"k": startKey, "n": int64(count)})
	if err != nil {
		return nil, err
	}
	return collect(ctx, res, count)
}

func (db *neoDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	var b strings.Builder
	fmt.Fprintf(&b, "MATCH (u:%s {ycsb_key: $k}) SET ", table)
	for i, p := range util.NewFieldPairs(values) {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "u.%s = $%s", p.Field, p.Field)
	}

	res, err := sessionOf(ctx).Run(ctx, b.String(), params(key, values))
	if err != nil {
		return err
	}
	// Autocommit only commits once the result is consumed, so skipping
	// this would report a write as done before the server has it.
	_, err = res.Consume(ctx)
	return err
}

func (db *neoDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE (u:%s {ycsb_key: $k", table)
	for _, p := range util.NewFieldPairs(values) {
		fmt.Fprintf(&b, ", %s: $%s", p.Field, p.Field)
	}
	b.WriteString("})")

	res, err := sessionOf(ctx).Run(ctx, b.String(), params(key, values))
	if err != nil {
		return err
	}
	_, err = res.Consume(ctx)
	return err
}

func (db *neoDB) Delete(ctx context.Context, table string, key string) error {
	q := fmt.Sprintf("MATCH (u:%s {ycsb_key: $k}) DETACH DELETE u", table)
	res, err := sessionOf(ctx).Run(ctx, q, map[string]any{"k": key})
	if err != nil {
		return err
	}
	_, err = res.Consume(ctx)
	return err
}

func init() {
	ycsb.RegisterDBCreator("neo4j", neoCreator{})
}

var _ ycsb.DB = (*neoDB)(nil)
