package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/hasura/go-graphql-client"
)

const (
	WORKERS              = 5                      // Number of concurrent writers.
	WORKER_LOOP_INTERVAL = 100 * time.Millisecond // Per-worker pacing between event inserts.
	MAX_EVENTS           = 100000                 // Upper bound on events to write before stopping.
	VALIDATION_INTERVAL  = 2500                   // Validate every N inserted events.
	VALIDATION_POLL      = 2 * time.Second        // Polling interval for validation scheduling.
)

var (
	count          uint64            = 0 // Monotonic event counter shared by workers.
	mu                               = sync.Mutex{}
	people                           = []string{"Alice", "Bob", "Charlie", "David", "Eve"} // Seed people IDs to link events to.
	insertTimeMu                     = sync.Mutex{}
	insertTime     map[int]time.Time // Tracks when each event value was submitted (for debugging gaps).
	servers        = []string{"http://localhost:8080", "http://localhost:8081", "http://localhost:8082"}
	missingCountMu = sync.Mutex{}
	missingCount   = map[string]int{} // Tracks missing event counts per server for reporting.

	// Custom HTTP client per server for increased connection limits
	httpClients = make([]*http.Client, len(servers))
)

// buildHTTPClient creates a custom HTTP client with sensible defaults for load testing.
// This addresses the default Go HTTP client's limited connection pool (MaxIdleConnsPerHost = 2).
func buildHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			MaxConnsPerHost:     100,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  false,
		},
	}
}

func main() {
	// Initialize custom HTTP clients per server for increased connection limits
	for i := range httpClients {
		httpClients[i] = buildHTTPClient()
	}

	// One GraphQL client per server, using the custom HTTP clients.
	clients := []*graphql.Client{}
	for i := range servers {
		clients = append(clients, graphql.NewClient(fmt.Sprintf("%s/graphql", servers[i]), httpClients[i]))
	}

	ctx := context.Background()

	// Ensure schema exists, then reset data and seed people.
	uploadSchemaIfRequired(ctx, clients[0])
	dropExistingData(servers[0])
	bootstrapPeople(ctx, clients[0], people)

	insertTime = make(map[int]time.Time, MAX_EVENTS)

	var wg sync.WaitGroup

	// Start concurrent writers.
	wg.Add(WORKERS)
	for i := 0; i < WORKERS; i++ {
		go runEventSimulator(ctx, &wg, clients, i+1)
	}

	// Start periodic progress reporting.
	wg.Add(1)
	go runReporting(ctx, &wg)

	// Expose Prometheus-style metrics for external monitoring
	go func() {
		if err := http.ListenAndServe(":3020", http.HandlerFunc(metrics)); err != nil {
			fmt.Printf("metrics server error: %v\n", err)
		}
	}()

	// Start background validation based on the event count.
	wg.Add(1)
	go runValidationScheduler(ctx, &wg, clients)

	wg.Wait()

	// Final validation pass once all writers stop.
	validate(ctx, clients, true)
}

func runReporting(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	start := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			elapsed := time.Since(start).Seconds()

			currentCount := getCount()
			rate := float64(currentCount) / elapsed
			remaining := MAX_EVENTS - currentCount
			estimation := int(float64(remaining) / rate)

			fmt.Printf("%s added %f events/s, total %d, estimated time remaining %d seconds\n", time.Now().Format(time.RFC3339), rate, currentCount, estimation)
			if currentCount >= MAX_EVENTS {
				return
			}
		}
	}
}

func runValidationScheduler(ctx context.Context, wg *sync.WaitGroup, clients []*graphql.Client) {
	defer wg.Done()

	ticker := time.NewTicker(VALIDATION_POLL)
	defer ticker.Stop()

	nextValidationAt := uint64(VALIDATION_INTERVAL)

	// Periodically check if enough events have been inserted to validate.
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for {
				currentCount := getCount()

				if currentCount >= MAX_EVENTS {
					return
				}

				if currentCount < nextValidationAt {
					break
				}

				fmt.Printf("starting validation at %d events\n", currentCount)
				validate(ctx, clients, false)
				nextValidationAt += uint64(VALIDATION_INTERVAL)
			}
		}
	}
}

func runEventSimulator(ctx context.Context, wg *sync.WaitGroup, clients []*graphql.Client, id int) {
	defer wg.Done()

	ticker := time.NewTicker(WORKER_LOOP_INTERVAL)
	defer ticker.Stop()

	errLog, err := os.OpenFile(fmt.Sprintf("error-%d.log", id), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer errLog.Close()

	// Write events on a fixed cadence until MAX_EVENTS is reached.
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if eventValue := addEvent(ctx, clients, errLog); eventValue >= MAX_EVENTS {
				return
			}
		}
	}
}

type PersonRef struct {
	ID *string `json:"id"`
}

type AddEventInput struct {
	Value  *uint64    `json:"value"`
	Person *PersonRef `json:"person"`
}

func addEvent(ctx context.Context, clients []*graphql.Client, errLog *os.File) uint64 {
	// Reserve the next event value.
	mu.Lock()
	value := count
	count++
	mu.Unlock()

	personID := people[randInt(len(people))]
	event := AddEventInput{
		Value: &value,
		Person: &PersonRef{
			ID: &personID,
		},
	}

	var mutation struct {
		AddEvent struct {
			Event struct {
				Value uint64 `graphql:"value"`
			} `graphql:"event"`
		} `graphql:"addEvent(input: $input)"`
	}

	variables := map[string]interface{}{
		"input": []AddEventInput{event},
	}

	// Randomize target server to spread writes across alphas.
	client := clients[randInt(len(clients))]

	// Track insert time for debugging gaps.
	insertTimeMu.Lock()
	insertTime[int(value)] = time.Now()
	insertTimeMu.Unlock()

	result, err := client.MutateRaw(ctx, &mutation, variables)
	if err != nil {
		if _, err := errLog.WriteString(fmt.Sprintf("%s: error adding event: %v\n", time.Now().Format(time.RFC3339), err)); err != nil {
			fmt.Printf("error writing to error log: %v\n", err)
		}
	}

	var response struct {
		AddEvent struct {
			Event []struct {
				Value uint64 `json:"value"`
			} `json:"event"`
		} `json:"addEvent"`
	}

	if err := json.Unmarshal(result, &response); err != nil {
		if _, err := errLog.WriteString(fmt.Sprintf("%s: error unmarshaling add event response: %v\n", time.Now().Format(time.RFC3339), err)); err != nil {
			fmt.Printf("error writing to error log: %v\n", err)
		}
	}

	if len(response.AddEvent.Event) != 1 {
		msg := fmt.Sprintf("%s: failed to add event (%d) response was %s\n", time.Now().Format(time.RFC3339), value, string(result))
		if _, err := errLog.WriteString(msg); err != nil {
			fmt.Print(msg)
		}
		return value
	}

	if response.AddEvent.Event[0].Value != value {
		bytes, err := json.Marshal(event)
		if err != nil {
			fmt.Printf("error marshaling event for logging: %v\n", err)
		}
		if _, err := errLog.WriteString(fmt.Sprintf("%s: event value mismatch: expected %d, got %d\n\n==Input==\n\n%s\n==Input End ==\n==Response==\n\n%s\n==Response End==\n", time.Now().Format(time.RFC3339), value, response.AddEvent.Event[0].Value, string(bytes), string(result))); err != nil {
			fmt.Printf("error writing to error log: %v\n", err)
		}
		panic(fmt.Sprintf("event value mismatch: expected %d, got %d. Write payload was %v", value, response.AddEvent.Event[0].Value, string(bytes)))
	}

	return value
}

func randInt(max int) int {
	n, err := rand.Int(rand.Reader, big.NewInt(int64(max)))
	if err != nil {
		panic(err)
	}
	return int(n.Int64())
}

func uploadSchemaIfRequired(ctx context.Context, client *graphql.Client) {
	// Probe schema by running a query; if it works, assume schema exists.
	var q struct {
		QueryEvent struct {
			Value uint64 `graphql:"value"`
		} `graphql:"queryEvent"`
	}

	variables := map[string]interface{}{}

	bytes, err := client.QueryRaw(ctx, &q, variables)
	if err == nil {
		var response struct {
			QueryEvent []struct {
				Value uint64 `json:"value"`
			} `json:"queryEvent"`
		}

		if err := json.Unmarshal(bytes, &response); err == nil {
			// Schema is present (empty results are OK).
			return
		}
	}

	// Upload schema to the admin endpoint when missing or incompatible.
	schemaFile := "./schema.sdl"
	schemaData, err := os.ReadFile(schemaFile)
	if err != nil {
		panic(fmt.Sprintf("failed to read schema file %s: %v", schemaFile, err))
	}

	req, err := http.NewRequest("POST", "http://localhost:8080/admin/schema", strings.NewReader(string(schemaData)))
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "application/sdl")

	httpClient := &http.Client{
		Timeout: time.Second,
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		panic(fmt.Sprintf("failed to upload schema: %s", resp.Status))
	}
	fmt.Println("uploaded schema successfully")
}

func bootstrapPeople(ctx context.Context, client *graphql.Client, personIDs []string) {
	// Ensure each person exists so events can reference them.
	var q struct {
		GetPerson struct {
			ID string `graphql:"id"`
		} `graphql:"getPerson(id: $id)"`
	}

	var mutation struct {
		AddPerson struct {
			Person []struct {
				ID string `graphql:"id"`
			} `graphql:"person"`
		} `graphql:"addPerson(input: $input)"`
	}

	type AddPersonInput struct {
		ID string `json:"id"`
	}

	input := []AddPersonInput{}
	type Result struct {
		GetPerson *struct {
			ID string `json:"id"`
		} `json:"getPerson"`
	}
	var clientResult Result
	for _, person := range personIDs {
		clientRaw, err := client.QueryRaw(ctx, &q, map[string]interface{}{"id": person})
		if err != nil {
			panic(err)
		}

		if err := json.Unmarshal(clientRaw, &clientResult); err != nil {
			panic(err)
		}

		if clientResult.GetPerson != nil {
			continue
		}

		input = append(input, AddPersonInput{ID: person})
	}

	if len(input) == 0 {
		return
	}

	variables := map[string]interface{}{
		"input": input,
	}
	_, err := client.MutateRaw(ctx, &mutation, variables)
	if err != nil {
		panic(err)
	}
}

func validate(ctx context.Context, clients []*graphql.Client, final bool) {
	const pageSize = 1000

	// Validate each server independently to detect gaps or duplicates.
	for i, client := range clients {
		highWaterMark, err := queryAggregateCount(ctx, client)
		if err != nil {
			fmt.Printf("failed to query aggregate count: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("validating alpha-%d: aggregate count = %d\n", i, highWaterMark)

		seen := make(map[int]bool, highWaterMark)
		for i := 0; i < highWaterMark; i++ {
			seen[i] = false
		}
		countSeen := 0
		outOfRange := 0
		missed := 0

		max := 0

		for offset := 0; ; offset += pageSize {
			// fmt.Printf("querying events with offset %d...\n", offset)
			events, err := queryEventPage(ctx, client, pageSize, offset)
			if err != nil {
				fmt.Printf("failed to query events (offset %d): %v\n", offset, err)
				os.Exit(1)
			}

			if len(events) == 0 {
				break
			}

			for _, event := range events {
				value := int(event.Value)

				if value < 0 {
					fmt.Printf("event value %d out of range [0, %d] likely gaps in the stream\n", value, highWaterMark)
					outOfRange++
					continue
				}

				if value >= highWaterMark && final {
					fmt.Printf("event value %d out of range [0, %d] likely gaps in the stream\n", value, highWaterMark)
					outOfRange++
					countSeen++
					continue
				} else if value >= highWaterMark && !final {
					continue
				}

				if seen[value] {
					fmt.Printf("duplicate event value %d detected\n", value)
					os.Exit(1)
				}

				seen[value] = true
				countSeen++

				if value > max {
					max = value
				}
			}
		}

		// Report missing values with their insert timestamps for debugging.
		for value, ok := range seen {
			if !ok {
				insertTimeMu.Lock()
				insertedAt := insertTime[value]
				insertTimeMu.Unlock()
				fmt.Printf("missing event value %d (inserted at %s)\n", value, insertedAt.Format(time.RFC3339))
				missed++
				continue
			}
		}

		if countSeen != highWaterMark {
			fmt.Printf("event count mismatch: aggregate=%d seen=%d outOfRange=%d missed=%d max=%d\n", highWaterMark, countSeen, outOfRange, missed, max)
			continue
		}

		passFail := "passed"
		if outOfRange > 0 || missed > 0 {
			passFail = "failed"
		}

		missingCountMu.Lock()
		missingCount[servers[i]] = missed
		missingCountMu.Unlock()

		fmt.Printf("validation %s: aggregate=%d seen=%d outOfRange=%d missed=%d max=%d\n", passFail, highWaterMark, countSeen, outOfRange, missed, max)
	}
}

func getCount() uint64 {
	// Locked read of the shared event counter.
	mu.Lock()
	defer mu.Unlock()
	return count
}

func queryAggregateCount(ctx context.Context, client *graphql.Client) (int, error) {
	// Query server-side aggregate count for expected high-water mark.
	var q struct {
		AggregateEvent struct {
			Count int `graphql:"count"`
		} `graphql:"aggregateEvent"`
	}

	bytes, err := client.QueryRaw(ctx, &q, map[string]interface{}{})
	if err != nil {
		return 0, err
	}

	var result struct {
		AggregateEvent struct {
			Count int `json:"count"`
		} `json:"aggregateEvent"`
	}

	if err := json.Unmarshal(bytes, &result); err != nil {
		return 0, err
	}

	return result.AggregateEvent.Count, nil
}

type eventRow struct {
	Value uint64 `json:"value"`
}

func queryEventPage(ctx context.Context, client *graphql.Client, first int, offset int) ([]eventRow, error) {
	// Page through events in stable order to find gaps/duplicates.
	var q struct {
		QueryEvent []eventRow `graphql:"queryEvent(first: $first, offset: $offset, order: {asc: value})"`
	}

	variables := map[string]interface{}{
		"first":  first,
		"offset": offset,
	}

	bytes, err := client.QueryRaw(ctx, &q, variables)
	if err != nil {
		return nil, err
	}

	var result struct {
		QueryEvent []eventRow `json:"queryEvent"`
	}

	if err := json.Unmarshal(bytes, &result); err != nil {
		return nil, err
	}

	return result.QueryEvent, nil
}

func dropExistingData(server string) {
	// Clear data before starting the load test.
	res, err := http.Post(fmt.Sprintf("%s/alter", server), "application/json", strings.NewReader(`{"drop_op": "DATA"}`))
	if err != nil {
		panic(fmt.Sprintf("failed to drop existing data: %v", err))
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		panic(fmt.Sprintf("failed to drop existing data: %s", res.Status))
	}
	fmt.Println("dropped existing data successfully")
}

func metrics(res http.ResponseWriter, req *http.Request) {
	// Simple text-based metrics endpoint for Prometheus scraping.
	mu.Lock()
	dbCount := count
	mu.Unlock()

	metricsHasInverseRelations := true
	metricsVersion := "v25.2.0"
	metricsSchema := "simple-with-an-inverse-relation"

	hasInverse := "false"
	if metricsHasInverseRelations {
		hasInverse = "true"
	}

	for server, missed := range missingCount {
		_, err := res.Write([]byte(fmt.Sprintf(`# HELP events_missing Number of events missing per server
# TYPE events_missing gauge
events_missing{server="%s"} %d
`, server, missed)))
		if err != nil {
			fmt.Printf("error writing metrics response: %v\n", err)
			return
		}
	}

	if _, err := res.Write([]byte(fmt.Sprintf(`# HELP events_total Number of events written
# TYPE events_total counter
events_total{version="%s",hasInverseRelations="%s",schema="%s"} %d`, metricsVersion, hasInverse, metricsSchema, dbCount))); err != nil {
		fmt.Printf("error writing metrics response: %v\n", err)
		return
	}
}
