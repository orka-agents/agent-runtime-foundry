package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
)

const foundryResourceScope = "https://ai.azure.com/.default"

type foundryTokenProvider interface {
	AccessToken(ctx context.Context) (string, error)
}

type azureFoundryTokenProvider struct {
	source azcore.TokenCredential
}

func newAzureFoundryTokenProvider() (*azureFoundryTokenProvider, error) {
	source, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("configure Azure credential: %w", err)
	}
	return &azureFoundryTokenProvider{source: source}, nil
}

func (p *azureFoundryTokenProvider) AccessToken(ctx context.Context) (string, error) {
	if p == nil || p.source == nil {
		return "", fmt.Errorf("azure credential is not configured")
	}
	token, err := p.source.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{foundryResourceScope}})
	if err != nil {
		return "", fmt.Errorf("acquire Azure access token: %w", err)
	}
	value := strings.TrimSpace(token.Token)
	if value == "" {
		return "", fmt.Errorf("azure credential returned an empty access token")
	}
	return value, nil
}
