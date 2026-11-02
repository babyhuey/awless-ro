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
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
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

// perItemIam lists every user in names and one access key per user, named
// "key-<user>", with the same denied and broken users as perItemSqs.
type perItemIam struct {
	IamAPI
	names  []string
	denied map[string]bool
	broken map[string]bool
}

func (i perItemIam) ListUsers(_ context.Context, _ *iam.ListUsersInput, _ ...func(*iam.Options)) (*iam.ListUsersOutput, error) {
	var users []iamtypes.User
	for _, n := range i.names {
		users = append(users, iamtypes.User{UserId: awssdk.String("id-" + n), UserName: awssdk.String(n)})
	}
	return &iam.ListUsersOutput{Users: users}, nil
}

func (i perItemIam) ListAccessKeys(_ context.Context, in *iam.ListAccessKeysInput, _ ...func(*iam.Options)) (*iam.ListAccessKeysOutput, error) {
	name := awssdk.ToString(in.UserName)
	switch {
	case i.denied[name]:
		return nil, apiError{code: "AccessDenied"}
	case i.broken[name]:
		return nil, apiError{code: "ServiceFailure"}
	}
	return &iam.ListAccessKeysOutput{AccessKeyMetadata: []iamtypes.AccessKeyMetadata{
		{AccessKeyId: awssdk.String("key-" + name), UserName: in.UserName},
	}}, nil
}

// A policy scoped to some users lets a role list them all and list keys only for
// its own. The other users' keys are left out, the fetch succeeds, and one
// warning names every user whose keys are missing.
func TestIamDeniedUserKeysAreSkippedAndWarnedOnce(t *testing.T) {
	api := perItemIam{
		names:  []string{"app", "ops-b", "ops-a"},
		denied: map[string]bool{"ops-a": true, "ops-b": true},
	}

	resources, warnings, err := fetchWithLog(t, NewConfig(&AWSAPI{Iam: api}), BuildAccessFetchFuncs, "accesskey")
	if err != nil {
		t.Fatalf("a user whose keys were denied failed the whole fetch: %v", err)
	}

	want := []string{"key-app"}
	if got := sortedIds(resources); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if len(warnings) != 1 {
		t.Fatalf("got %d warnings %q, want exactly one for the whole fetch", len(warnings), warnings)
	}
	if !strings.Contains(warnings[0], "ListAccessKeys") || !strings.Contains(warnings[0], "ops-a, ops-b") {
		t.Errorf("warning %q should name the call and the skipped users, sorted", warnings[0])
	}
}

func TestIamAnyOtherErrorStillFailsTheFetch(t *testing.T) {
	api := perItemIam{
		names:  []string{"a", "b"},
		denied: map[string]bool{"a": true},
		broken: map[string]bool{"b": true},
	}

	if _, _, err := fetchWithLog(t, NewConfig(&AWSAPI{Iam: api}), BuildAccessFetchFuncs, "accesskey"); err == nil {
		t.Fatal("an error that is not access denied was swallowed")
	}
}

// perItemKms lists every key in ids and describes each by id, with the same
// denied keys as perItemSqs.
type perItemKms struct {
	KmsAPI
	ids    []string
	denied map[string]bool
}

func (k perItemKms) ListAliases(_ context.Context, _ *kms.ListAliasesInput, _ ...func(*kms.Options)) (*kms.ListAliasesOutput, error) {
	return &kms.ListAliasesOutput{}, nil
}

func (k perItemKms) ListKeys(_ context.Context, _ *kms.ListKeysInput, _ ...func(*kms.Options)) (*kms.ListKeysOutput, error) {
	var keys []kmstypes.KeyListEntry
	for _, id := range k.ids {
		keys = append(keys, kmstypes.KeyListEntry{KeyId: awssdk.String(id), KeyArn: awssdk.String("arn:aws:kms:eu-west-1:1:key/" + id)})
	}
	return &kms.ListKeysOutput{Keys: keys}, nil
}

func (k perItemKms) DescribeKey(_ context.Context, in *kms.DescribeKeyInput, _ ...func(*kms.Options)) (*kms.DescribeKeyOutput, error) {
	id := awssdk.ToString(in.KeyId)
	if k.denied[id] {
		return nil, apiError{code: "AccessDeniedException"}
	}
	return &kms.DescribeKeyOutput{KeyMetadata: &kmstypes.KeyMetadata{KeyId: in.KeyId, Arn: awssdk.String("arn:aws:kms:eu-west-1:1:key/" + id)}}, nil
}

// A key policy can deny DescribeKey on some keys. Those keys are still listed,
// as before, and now one warning names them, as for queues and tables.
func TestKmsDeniedKeyIsKeptAndWarnedOnce(t *testing.T) {
	api := perItemKms{
		ids:    []string{"app", "ops-b", "ops-a"},
		denied: map[string]bool{"ops-a": true, "ops-b": true},
	}

	resources, warnings, err := fetchWithLog(t, NewConfig(&AWSAPI{Kms: api}), BuildKmsFetchFuncs, "kmskey")
	if err != nil {
		t.Fatalf("a key whose description was denied failed the whole fetch: %v", err)
	}

	want := []string{"app", "ops-a", "ops-b"}
	if got := sortedIds(resources); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if len(warnings) != 1 {
		t.Fatalf("got %d warnings %q, want exactly one for the whole fetch", len(warnings), warnings)
	}
	if !strings.Contains(warnings[0], "DescribeKey") || !strings.Contains(warnings[0], "ops-a, ops-b") {
		t.Errorf("warning %q should name the call and the denied keys, sorted", warnings[0])
	}
}

// The warning appears only when something was denied.
func TestNoWarningWhenNothingIsDenied(t *testing.T) {
	_, warnings, err := fetchWithLog(t, NewConfig(&AWSAPI{Kms: perItemKms{ids: []string{"a", "b"}}}), BuildKmsFetchFuncs, "kmskey")
	if err != nil {
		t.Fatal(err)
	}
	if len(warnings) != 0 {
		t.Errorf("got warnings %q, want none", warnings)
	}
}
