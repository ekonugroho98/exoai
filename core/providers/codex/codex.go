// Package codex exposes Codex as a first-class OpenAI-compatible provider.
package codex

import (
	"github.com/maximhq/bifrost/core/providers/openai"
	schemas "github.com/maximhq/bifrost/core/schemas"
)

// NewCodexProvider creates a Codex provider backed by the OpenAI-compatible
// provider implementation. Codex account tokens/API keys use the same wire
// format as OpenAI, but keeping a distinct provider key lets users configure
// and route codex/* models separately.
func NewCodexProvider(config *schemas.ProviderConfig, logger schemas.Logger) *openai.OpenAIProvider {
	if config.CustomProviderConfig == nil {
		config.CustomProviderConfig = &schemas.CustomProviderConfig{}
	}
	config.CustomProviderConfig.CustomProviderKey = string(schemas.Codex)
	config.CustomProviderConfig.BaseProviderType = schemas.OpenAI
	return openai.NewOpenAIProvider(config, logger)
}
