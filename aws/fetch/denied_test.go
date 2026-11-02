package awsfetch

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/theazz/awless-ro/cloud/properties"
	"github.com/theazz/awless-ro/fetch"
	"github.com/theazz/awless-ro/graph"
	"github.com/theazz/awless-ro/logger"
)

// perItemSqs lists every queue in urls and describes each by URL. A URL in denied
// answers the way a resource-scoped policy does; one in broken fails for another reason.
type perItemSqs struct {
	SqsAPI
	urls   []string
	denied map[string]bool
	broken map[string]bool
	gone   map[string]bool
}

func (s perItemSqs) ListQueues(_ context.Context, _ *sqs.ListQueuesInput, _ ...func(*sqs.Options)) (*sqs.ListQueuesOutput, error) {
	return &sqs.ListQueuesOutput{QueueUrls: s.urls}, nil
}

func (s perItemSqs) GetQueueAttributes(_ context.Context, in *sqs.GetQueueAttributesInput, _ ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	url := awssdk.ToString(in.QueueUrl)
	switch {
	case s.denied[url]:
		return nil, apiError{code: "AccessDenied"}
	case s.broken[url]:
		return nil, apiError{code: "InternalError"}
	case s.gone[url]:
		return nil, &sqstypes.QueueDoesNotExist{}
	}
	return &sqs.GetQueueAttributesOutput{Attributes: map[string]string{"QueueArn": "arn:aws:sqs:eu-west-1:1:" + url}}, nil
}

// perItemDynamodb lists every name and describes each by name, with the same
// denied and broken tables as perItemSqs.
type perItemDynamodb struct {
	DynamodbAPI
	names  []string
	denied map[string]bool
	broken map[string]bool
}

func (d perItemDynamodb) ListTables(_ context.Context, _ *dynamodb.ListTablesInput, _ ...func(*dynamodb.Options)) (*dynamodb.ListTablesOutput, error) {
	return &dynamodb.ListTablesOutput{TableNames: d.names}, nil
}

func (d perItemDynamodb) DescribeTable(_ context.Context, in *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	name := awssdk.ToString(in.TableName)
	switch {
	case d.denied[name]:
		return nil, apiError{code: "AccessDeniedException"}
	case d.broken[name]:
		return nil, apiError{code: "InternalServerError"}
	}
	return &dynamodb.DescribeTableOutput{Table: &dynamodbtypes.TableDescription{
		TableName: in.TableName,
		TableArn:  awssdk.String("arn:aws:dynamodb:eu-west-1:1:table/" + name),
	}}, nil
}

// fetchWithLog runs one fetcher with its logger writing to a buffer and returns what
// the fetcher returned along with every line it logged.
func fetchWithLog(t *testing.T, conf *Config, build func(*Config) fetch.Funcs, name string) ([]*graph.Resource, []string, error) {
	t.Helper()
	var buf bytes.Buffer
	conf.Log = logger.New("", 0, &buf)
	resources, _, err := build(conf)[name](context.Background(), nil)
	var lines []string
	for _, l := range strings.Split(buf.String(), "\n") {
		if l != "" {
			lines = append(lines, l)
		}
	}
	return resources, lines, err
}

func sortedIds(resources []*graph.Resource) []string {
	got := ids(resources)
	sort.Strings(got)
	return got
}

// A policy scoped to some queues lets a role list them all and describe only its
// own. The queues it cannot describe keep their URL from ListQueues, the fetch
// succeeds, and one warning says how many were affected instead of one per queue.
func TestSqsDeniedQueueIsKeptAndWarnedOnce(t *testing.T) {
	api := perItemSqs{
		urls:   []string{"https://q/app-orders", "https://q/ops-a", "https://q/ops-b"},
		denied: map[string]bool{"https://q/ops-a": true, "https://q/ops-b": true},
	}

	resources, warnings, err := fetchWithLog(t, NewConfig(&AWSAPI{Sqs: api}), BuildMessagingFetchFuncs, "queue")
	if err != nil {
		t.Fatalf("a queue whose attributes were denied failed the whole fetch: %v", err)
	}

	want := []string{"https://q/app-orders", "https://q/ops-a", "https://q/ops-b"}
	if got := sortedIds(resources); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for _, r := range resources {
		_, hasArn := r.Properties()[properties.Arn]
		if denied := api.denied[r.Id()]; denied == hasArn {
			t.Errorf("%s: has ARN %v, but denied is %v; only queues that could be described carry attributes", r.Id(), hasArn, denied)
		}
	}
	if len(warnings) != 1 {
		t.Fatalf("got %d warnings %q, want exactly one for the whole fetch", len(warnings), warnings)
	}
	if !strings.Contains(warnings[0], "GetQueueAttributes") || !strings.Contains(warnings[0], "2 queue(s)") {
		t.Errorf("warning %q should name the call and say 2 queues were denied", warnings[0])
	}
}

// A queue deleted between ListQueues and GetQueueAttributes is skipped, as before,
// and is not mistaken for a denied one: no warning.
func TestSqsQueueDeletedAfterListingIsSkippedWithoutWarning(t *testing.T) {
	api := perItemSqs{
		urls: []string{"https://q/app-orders", "https://q/gone"},
		gone: map[string]bool{"https://q/gone": true},
	}

	resources, warnings, err := fetchWithLog(t, NewConfig(&AWSAPI{Sqs: api}), BuildMessagingFetchFuncs, "queue")
	if err != nil {
		t.Fatalf("a queue deleted after listing failed the whole fetch: %v", err)
	}

	want := []string{"https://q/app-orders"}
	if got := sortedIds(resources); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if len(warnings) != 0 {
		t.Errorf("got warnings %q, want none for a deleted queue", warnings)
	}
}

func TestSqsAnyOtherErrorStillFailsTheFetch(t *testing.T) {
	api := perItemSqs{
		urls:   []string{"https://q/a", "https://q/b"},
		denied: map[string]bool{"https://q/a": true},
		broken: map[string]bool{"https://q/b": true},
	}

	if _, _, err := fetchWithLog(t, NewConfig(&AWSAPI{Sqs: api}), BuildMessagingFetchFuncs, "queue"); err == nil {
		t.Fatal("an error that is not access denied was swallowed")
	}
}

// ListTables returns names only, so a table DescribeTable denied has no ARN to
// identify it by and is left out. The fetch still succeeds and one warning names
// every table that was left out.
func TestDynamodbDeniedTableIsSkippedAndWarnedOnce(t *testing.T) {
	api := perItemDynamodb{
		names:  []string{"app-orders", "ops-b", "ops-a"},
		denied: map[string]bool{"ops-a": true, "ops-b": true},
	}

	resources, warnings, err := fetchWithLog(t, NewConfig(&AWSAPI{Dynamodb: api}), BuildDynamodbFetchFuncs, "table")
	if err != nil {
		t.Fatalf("a table whose description was denied failed the whole fetch: %v", err)
	}

	want := []string{"arn:aws:dynamodb:eu-west-1:1:table/app-orders"}
	if got := sortedIds(resources); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if len(warnings) != 1 {
		t.Fatalf("got %d warnings %q, want exactly one for the whole fetch", len(warnings), warnings)
	}
	if !strings.Contains(warnings[0], "DescribeTable") || !strings.Contains(warnings[0], "ops-a, ops-b") {
		t.Errorf("warning %q should name the call and the skipped tables, sorted", warnings[0])
	}
}

func TestDynamodbAnyOtherErrorStillFailsTheFetch(t *testing.T) {
	api := perItemDynamodb{
		names:  []string{"a", "b"},
		denied: map[string]bool{"a": true},
		broken: map[string]bool{"b": true},
	}

	if _, _, err := fetchWithLog(t, NewConfig(&AWSAPI{Dynamodb: api}), BuildDynamodbFetchFuncs, "table"); err == nil {
		t.Fatal("an error that is not access denied was swallowed")
	}
}
