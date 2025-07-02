package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/pricing"
	"github.com/aws/aws-sdk-go-v2/service/pricing/types"
)

func EstimateS3ListRequestCost(ctx context.Context, region string, requestCount int) (float64, error) {
	cfg, err := func() (aws.Config, error) {
		if region != "" {
			return config.LoadDefaultConfig(ctx, config.WithRegion(region))
		}
		return config.LoadDefaultConfig(ctx)
	}()
	if err != nil {
		return 0, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := pricing.NewFromConfig(cfg)

	input := &pricing.GetProductsInput{
		ServiceCode: aws.String("AmazonS3"),
		Filters: []types.Filter{
			{
				Type:  types.FilterTypeTermMatch,
				Field: aws.String("group"),
				Value: aws.String("AmazonS3-ListRequests"),
			},
		},
		FormatVersion: aws.String("aws_v1"),
		MaxResults:    aws.Int32(1),
	}

	result, err := client.GetProducts(ctx, input)
	if err != nil || len(result.PriceList) == 0 {
		return 0, fmt.Errorf("failed to get pricing data: %w", err)
	}

	// Parse returned JSON price
	var priceItem map[string]any
	if err := json.Unmarshal([]byte(result.PriceList[0]), &priceItem); err != nil {
		return 0, fmt.Errorf("failed to parse price list: %w", err)
	}

	// Drill down into pricing
	terms := priceItem["terms"].(map[string]any)["OnDemand"].(map[string]any)
	var pricePer1000 float64

	for _, v := range terms {
		priceDimensions := v.(map[string]any)["priceDimensions"].(map[string]any)
		for _, dim := range priceDimensions {
			priceStr := dim.(map[string]any)["pricePerUnit"].(map[string]any)["USD"].(string)
			pricePer1000, err = strconv.ParseFloat(priceStr, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid price format: %w", err)
			}
			break
		}
		break
	}

	if pricePer1000 == 0 {
		return 0, errors.New("price could not be extracted")
	}

	// Compute cost
	totalCost := (float64(requestCount) / 1000.0) * pricePer1000
	return totalCost, nil
}
