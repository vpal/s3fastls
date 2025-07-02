package main

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
)

func TestEstimateS3ListRequestCost_Basic(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Check for valid AWS credentials using the SDK's default chain
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		t.Skipf("skipping: could not load AWS config: %v", err)
	}
	creds, err := cfg.Credentials.Retrieve(ctx)
	if err != nil || creds.AccessKeyID == "" || creds.SecretAccessKey == "" {
		t.Skip("skipping: no valid AWS credentials found in environment or config")
	}

	testCases := []struct {
		name   string
		region string
		count  int
	}{
		{"us-east-1, 1000", "us-east-1", 1000},
		{"us-west-2, 10000", "us-west-2", 10000},
		{"default region, 5000", "", 5000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cost, err := EstimateS3ListRequestCost(ctx, tc.region, tc.count)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if cost < 0 {
				t.Errorf("cost should not be negative, got %f", cost)
			}
		})
	}
}
