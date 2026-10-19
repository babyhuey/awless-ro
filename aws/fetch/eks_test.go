package awsfetch

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"testing"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"
)

// pagedEks answers ListClusters one page of names at a time and DescribeCluster
// for any name it listed. A name in gone answers the way EKS does for a cluster
// deleted after it was listed.
type pagedEks struct {
	EksAPI
	pages [][]string
	gone  map[string]bool
}

func (p pagedEks) ListClusters(_ context.Context, in *eks.ListClustersInput, _ ...func(*eks.Options)) (*eks.ListClustersOutput, error) {
	idx := tokenIndex(awssdk.ToString(in.NextToken))
	out := &eks.ListClustersOutput{Clusters: p.pages[idx]}
	if idx+1 < len(p.pages) {
		out.NextToken = awssdk.String(strconv.Itoa(idx + 1))
	}
	return out, nil
}

func (p pagedEks) DescribeCluster(_ context.Context, in *eks.DescribeClusterInput, _ ...func(*eks.Options)) (*eks.DescribeClusterOutput, error) {
	name := awssdk.ToString(in.Name)
	if p.gone[name] {
		return nil, &ekstypes.ResourceNotFoundException{Message: awssdk.String("No cluster found for name: " + name + ".")}
	}
	return &eks.DescribeClusterOutput{Cluster: &ekstypes.Cluster{
		Name: in.Name,
		Arn:  awssdk.String("arn:aws:eks:eu-west-1:1:cluster/" + name),
	}}, nil
}

// ListClusters returns names only, so every name on every page has to be
// described for its cluster to be listed.
func TestEksClustersAreDescribedAcrossEveryPage(t *testing.T) {
	api := pagedEks{pages: [][]string{{"a", "b"}, {"c"}}}
	funcs := BuildEksFetchFuncs(NewConfig(&AWSAPI{Eks: api}))
	got := ids(runFetcher(t, funcs, "ekscluster"))
	sort.Strings(got)

	want := []string{
		"arn:aws:eks:eu-west-1:1:cluster/a",
		"arn:aws:eks:eu-west-1:1:cluster/b",
		"arn:aws:eks:eu-west-1:1:cluster/c",
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// A cluster deleted between ListClusters and DescribeCluster is left out
// instead of failing the whole listing.
func TestEksClusterDeletedAfterListingIsSkipped(t *testing.T) {
	api := pagedEks{pages: [][]string{{"a", "b"}, {"c"}}, gone: map[string]bool{"b": true}}
	funcs := BuildEksFetchFuncs(NewConfig(&AWSAPI{Eks: api}))
	got := ids(runFetcher(t, funcs, "ekscluster"))
	sort.Strings(got)

	want := []string{
		"arn:aws:eks:eu-west-1:1:cluster/a",
		"arn:aws:eks:eu-west-1:1:cluster/c",
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
