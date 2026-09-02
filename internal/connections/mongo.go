package connections

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// mongoURI assembles the connection string from the same variables
// worker_service uses, named individually rather than as one blob so a missing
// one can be reported by name.
//
// MONGODB_URI overrides the whole thing when set. The assembled form is
// mongodb+srv://, which is what Atlas needs and what a plain mongod cannot
// satisfy - the scheme means "resolve these hosts via SRV DNS records", and a
// container has none. Without the override this service could not be run
// against a local Mongo at all, which is the same trap user_service had.
//
// Credentials are escaped: a password containing @ / : or ? would otherwise be
// read as part of the host or the query string.
func mongoURI() (string, error) {
	if uri := os.Getenv("MONGODB_URI"); uri != "" {
		return uri, nil
	}
	host := os.Getenv("MONGODB_CLUSTER_HOST")
	user := os.Getenv("MONGODB_CLUSTER_USER")
	password := os.Getenv("MONGODB_CLUSTER_PASS")

	var missing []string
	if host == "" {
		missing = append(missing, "MONGODB_CLUSTER_HOST")
	}
	if user == "" {
		missing = append(missing, "MONGODB_CLUSTER_USER")
	}
	if password == "" {
		missing = append(missing, "MONGODB_CLUSTER_PASS")
	}
	if len(missing) > 0 {
		return "", fmt.Errorf("mongo config incomplete: missing %s", strings.Join(missing, ", "))
	}

	return fmt.Sprintf(
		"mongodb+srv://%s:%s@%s/?retryWrites=true&w=majority",
		url.QueryEscape(user),
		url.QueryEscape(password),
		host,
	), nil
}

func openMongo(ctx context.Context) (*mongo.Database, error) {
	uri, err := mongoURI()
	if err != nil {
		return nil, err
	}

	connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(connectCtx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("mongo connect: %w", err)
	}
	if err := client.Ping(connectCtx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("mongo unreachable: %w", err)
	}

	name := os.Getenv("MONGODB_DB")
	if name == "" {
		name = "chatterloop"
	}
	return client.Database(name), nil
}
