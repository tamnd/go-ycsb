package neo4j

// Build with -tags neo4j. Nothing has to be installed locally, the driver
// is pure Go, but a server has to be reachable. The quickest one:
//
//	docker run --rm -d --name neo4j-ycsb \
//	  -p 7474:7474 -p 7687:7687 \
//	  -e NEO4J_AUTH=neo4j/benchpass \
//	  -e NEO4J_server_memory_heap_max__size=4G \
//	  -e NEO4J_server_memory_pagecache_size=4G \
//	  neo4j:2026.07.1
//
// Then point the run at it:
//
//	-p neo4j.uri=bolt://127.0.0.1:7687 -p neo4j.password=benchpass
//
// Keep the container on the same host as the harness. Neo4j is the only
// engine here that cannot be embedded, and putting a real network between
// the two would turn the result into a measurement of the network.
