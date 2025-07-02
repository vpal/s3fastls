package main

import (
	"context"
	"testing"
)

func TestEstimateS3ListRequestCost_Basic(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
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
