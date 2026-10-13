package awsfetch

import (
	"context"
	"fmt"
	"testing"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	cloudtrailtypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
)

// shadowingCloudtrail answers DescribeTrails the way CloudTrail does: unless asked
// not to, it also returns the shadow copy of every multi-region trail homed in
// another region.
type shadowingCloudtrail struct {
	CloudtrailAPI
}

func (shadowingCloudtrail) DescribeTrails(_ context.Context, in *cloudtrail.DescribeTrailsInput, _ ...func(*cloudtrail.Options)) (*cloudtrail.DescribeTrailsOutput, error) {
	trails := []cloudtrailtypes.Trail{
		{TrailARN: awssdk.String("arn:aws:cloudtrail:eu-west-1:1:trail/local"), HomeRegion: awssdk.String("eu-west-1")},
	}
	if in.IncludeShadowTrails == nil || *in.IncludeShadowTrails {
		trails = append(trails, cloudtrailtypes.Trail{TrailARN: awssdk.String("arn:aws:cloudtrail:us-east-1:1:trail/org"), HomeRegion: awssdk.String("us-east-1"), IsMultiRegionTrail: awssdk.Bool(true)})
	}
	return &cloudtrail.DescribeTrailsOutput{TrailList: trails}, nil
}

// A multi-region trail is reported by every region it covers, under the same ARN.
// Syncing each region would then file one trail under several regions, so trails
// are fetched only in their home region.
func TestTrailsAreFetchedOnlyInTheirHomeRegion(t *testing.T) {
	funcs := BuildCloudtrailFetchFuncs(NewConfig(&AWSAPI{Cloudtrail: shadowingCloudtrail{}}))
	got := ids(runFetcher(t, funcs, "trail"))

	want := []string{"arn:aws:cloudtrail:eu-west-1:1:trail/local"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("got %v, want %v: shadow trails from other regions were included", got, want)
	}
}
