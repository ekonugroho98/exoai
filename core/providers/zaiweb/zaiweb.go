// Package zaiweb implements the Z.ai web (chat.z.ai) provider.
//
// This provider proxies to chat.z.ai's reverse-engineered web API endpoint.
// Unlike Z.ai's official API (api.z.ai / open.bigmodel.cn), the web endpoint:
//
//   - accepts a minimal OpenAI-style body: {stream, model, messages}
//   - requires the `X-FE-Version` header (server enforces a minimum frontend
//     version; without it the server returns error code 426 inside an SSE chunk)
//   - emits a CUSTOM SSE format (not OpenAI-compatible):
//     data: {"type":"chat:completion","data":{"delta_content":"...","phase":"answer"}}
//     data: {"type":"chat:completion","data":{"phase":"other","usage":{...}}}
//     data: {"type":"chat:completion","data":{"phase":"done","done":true}}
//
// Phase values observed:
//   - "thinking": chain-of-thought reasoning (only when features.enable_thinking=true);
//     mapped to BifrostStreamChunk delta.Reasoning.
//   - "answer":   final response content; mapped to delta.Content.
//   - "other":    metadata chunks. Carries usage (token counts) and edit_index/
//     edit_content (retroactive content corrections).
//   - "done":     end-of-stream marker.
//
// Authentication is via Bearer token sourced from the chat.z.ai cookie of a
// logged-in browser session. Tokens have a server-side TTL so this is intended
// for learning/experimentation, not production use.
package zaiweb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"github.com/google/uuid"
	"github.com/valyala/fasthttp"

	providerUtils "github.com/maximhq/bifrost/core/providers/utils"
	schemas "github.com/maximhq/bifrost/core/schemas"
)

// generateChatCompletionID returns an OpenAI-style chat completion ID like
// "chatcmpl-<uuid>". Z.ai's web SSE doesn't include a server-generated ID, so
// we synthesize one for downstream compatibility.
func generateChatCompletionID() string {
	return "chatcmpl-" + uuid.New().String()
}

const (
	// defaultBaseURL is the chat.z.ai web API root.
	defaultBaseURL = "https://chat.z.ai"

	// chatCompletionsPath is the v2 web completions endpoint.
	chatCompletionsPath = "/api/v2/chat/completions"

	// defaultFEVersion is the value sent in the X-FE-Version header.
	// The server enforces a minimum (>= 1.0.0). Any plausible value works
	// today, but matching the value the real Z.ai frontend sends keeps us
	// indistinguishable from a real browser. Bump to whatever the live UI
	// reports (DevTools → Network → any /api/v2/* request → x-fe-version).
	defaultFEVersion = "prod-fe-1.1.30"

	// objectChatCompletionChunk is the OpenAI-style streaming object marker.
	objectChatCompletionChunk = "chat.completion.chunk"

	// objectChatCompletion is the OpenAI-style non-streaming object marker.
	objectChatCompletion = "chat.completion"
)

// ZaiWebProvider implements the Provider interface for the chat.z.ai web API.
//
// Two transport modes are supported:
//   - direct: POST directly to chat.z.ai with a reverse-engineered Bearer
//     token and X-FE-Version header. Subject to FRONTEND_CAPTCHA and to
//     server-side signature checks that change with frontend releases.
//   - browser-pool: forward the request to a local Python worker
//     (zai_browser_worker.py) that drives the real chat UI inside a
//     logged-in Camoufox session. Signature/captcha/refresh-token are
//     handled inside the browser. Enable by setting the env var
//     BIFROST_ZAI_BROWSER_POOL_URL, e.g. "http://127.0.0.1:9001".
type ZaiWebProvider struct {
	logger              schemas.Logger
	client              *fasthttp.Client
	streamingClient     *fasthttp.Client
	networkConfig       schemas.NetworkConfig
	sendBackRawRequest  bool
	sendBackRawResponse bool
	feVersion           string
	browserPoolURL      string // empty = direct mode; non-empty = pool mode
}

// NewZaiWebProvider creates a new ZaiWeb provider instance.
func NewZaiWebProvider(config *schemas.ProviderConfig, logger schemas.Logger) (*ZaiWebProvider, error) {
	config.CheckAndSetDefaults()

	requestTimeout := time.Second * time.Duration(config.NetworkConfig.DefaultRequestTimeoutInSeconds)
	client := &fasthttp.Client{
		ReadTimeout:         requestTimeout,
		WriteTimeout:        requestTimeout,
		MaxConnsPerHost:     config.NetworkConfig.MaxConnsPerHost,
		MaxIdleConnDuration: 30 * time.Second,
		MaxConnWaitTimeout:  requestTimeout,
		MaxConnDuration:     time.Second * time.Duration(schemas.DefaultMaxConnDurationInSeconds),
		ConnPoolStrategy:    fasthttp.FIFO,
	}

	client = providerUtils.ConfigureProxy(client, config.ProxyConfig, logger)
	client = providerUtils.ConfigureDialer(client)
	client = providerUtils.ConfigureTLS(client, config.NetworkConfig, logger)
	streamingClient := providerUtils.BuildStreamingClient(client)

	if config.NetworkConfig.BaseURL == "" {
		config.NetworkConfig.BaseURL = defaultBaseURL
	}
	config.NetworkConfig.BaseURL = strings.TrimRight(config.NetworkConfig.BaseURL, "/")

	// Allow overriding X-FE-Version via ExtraHeaders if user wants to pin a
	// specific version; otherwise use the default.
	feVersion := defaultFEVersion
	if config.NetworkConfig.ExtraHeaders != nil {
		if v, ok := config.NetworkConfig.ExtraHeaders["X-FE-Version"]; ok && v != "" {
			feVersion = v
		}
	}

	// Optional browser-pool transport. When set, all chat completion
	// requests are forwarded to this URL's /chat endpoint (see
	// transports/bifrost-http/handlers/zai_browser_worker.py) instead of
	// going to chat.z.ai directly. The worker handles authentication,
	// captcha, signature, and token refresh inside a real browser.
	browserPoolURL := strings.TrimRight(os.Getenv("BIFROST_ZAI_BROWSER_POOL_URL"), "/")
	if browserPoolURL != "" {
		logger.Info(fmt.Sprintf("zaiweb: routing through browser pool at %s", browserPoolURL))
	}

	return &ZaiWebProvider{
		logger:              logger,
		client:              client,
		streamingClient:     streamingClient,
		networkConfig:       config.NetworkConfig,
		sendBackRawRequest:  config.SendBackRawRequest,
		sendBackRawResponse: config.SendBackRawResponse,
		feVersion:           feVersion,
		browserPoolURL:      browserPoolURL,
	}, nil
}

// GetProviderKey returns the provider identifier for ZaiWeb.
func (provider *ZaiWebProvider) GetProviderKey() schemas.ModelProvider {
	return schemas.ZaiWeb
}

// ----------------------------------------------------------------------------
// Z.ai wire types
// ----------------------------------------------------------------------------

// zaiChatRequest is the JSON payload Z.ai's web API accepts.
//
// Minimum viable body discovered empirically:
//
//	{"stream":true,"model":"GLM-5.1","messages":[{"role":"user","content":"hi"}]}
//
// The Features struct is optional — used to toggle chain-of-thought.
type zaiChatRequest struct {
	Stream   bool             `json:"stream"`
	Model    string           `json:"model"`
	Messages []zaiChatMessage `json:"messages"`
	Features *zaiFeatures     `json:"features,omitempty"`
}

type zaiChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type zaiFeatures struct {
	EnableThinking bool `json:"enable_thinking,omitempty"`
}

// zaiSSEEvent is one Z.ai SSE chunk envelope:
//
//	{"type":"chat:completion","data":{...}}
type zaiSSEEvent struct {
	Type string           `json:"type"`
	Data zaiSSEEvent_Data `json:"data"`
}

// zaiSSEEvent_Data is the inner payload. Different fields are populated for
// different phases.
type zaiSSEEvent_Data struct {
	DeltaContent string    `json:"delta_content,omitempty"`
	Phase        string    `json:"phase,omitempty"`
	Done         bool      `json:"done,omitempty"`
	EditIndex    *int      `json:"edit_index,omitempty"`
	EditContent  string    `json:"edit_content,omitempty"`
	Usage        *zaiUsage `json:"usage,omitempty"`
	// Error reporting nested envelope (observed when X-FE-Version is missing):
	// {"data":{"error":{"detail":"...","code":426},"done":true}}
	Error *zaiError `json:"error,omitempty"`
}

type zaiUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type zaiError struct {
	Detail string `json:"detail"`
	Code   int    `json:"code"`
}

// ----------------------------------------------------------------------------
// Request/response transformation
// ----------------------------------------------------------------------------

// toZaiRequest converts a Bifrost chat request into Z.ai's wire format.
// Z.ai only accepts a flat messages array with string content. Multi-modal
// content (image blocks etc.) is best-effort flattened to text.
func toZaiRequest(req *schemas.BifrostChatRequest, stream bool) (*zaiChatRequest, error) {
	if req == nil {
		return nil, errors.New("nil request")
	}
	out := &zaiChatRequest{
		Stream:   stream,
		Model:    req.Model,
		Messages: make([]zaiChatMessage, 0, len(req.Input)),
	}
	for _, m := range req.Input {
		out.Messages = append(out.Messages, zaiChatMessage{
			Role:    string(m.Role),
			Content: flattenContent(m.Content),
		})
	}

	// Decide whether to enable Z.ai's chain-of-thought "thinking" mode.
	//
	// Multiple inputs supported, in priority order:
	//   1. Bifrost's native Params.Reasoning (set via top-level `reasoning_effort`
	//      or a `reasoning` object in the OpenAI-compat request).
	//      Enabled when Reasoning.Enabled==true OR Reasoning.Effort != "none".
	//   2. Explicit Params.ExtraParams["enable_thinking"] = true (escape hatch
	//      for clients that don't speak the reasoning fields).
	if req.Params != nil {
		if reasoningEnabled(req.Params.Reasoning) {
			out.Features = &zaiFeatures{EnableThinking: true}
		} else if req.Params.ExtraParams != nil {
			if v, ok := req.Params.ExtraParams["enable_thinking"]; ok {
				if b, ok := v.(bool); ok && b {
					out.Features = &zaiFeatures{EnableThinking: true}
				}
			}
		}
	}

	return out, nil
}

// reasoningEnabled returns true if the Bifrost ChatReasoning struct asks for
// reasoning to be on. We treat any non-"none" effort as a request to enable it.
func reasoningEnabled(r *schemas.ChatReasoning) bool {
	if r == nil {
		return false
	}
	if r.Enabled != nil && *r.Enabled {
		return true
	}
	if r.Effort != nil && *r.Effort != "" && *r.Effort != "none" {
		return true
	}
	return false
}

// flattenContent reduces a ChatMessageContent (which may be a string or a list
// of typed blocks) into a single string suitable for Z.ai's web API.
func flattenContent(c *schemas.ChatMessageContent) string {
	if c == nil {
		return ""
	}
	if c.ContentStr != nil {
		return *c.ContentStr
	}
	var b strings.Builder
	for _, blk := range c.ContentBlocks {
		if blk.Text != nil && *blk.Text != "" {
			if b.Len() > 0 {
				b.WriteString("\n")
			}
			b.WriteString(*blk.Text)
		}
	}
	return b.String()
}

// buildHTTPRequest sets up the fasthttp request with auth + headers.
func (provider *ZaiWebProvider) buildHTTPRequest(ctx *schemas.BifrostContext, body []byte, key schemas.Key, streaming bool) *fasthttp.Request {
	req := fasthttp.AcquireRequest()
	req.Header.SetMethod(http.MethodPost)
	req.Header.SetContentType("application/json")

	// Browser-pool transport: the Python worker owns the session, so we
	// skip the X-FE-Version dance and the Bearer token. The worker accepts
	// the same {stream, model, messages, features} body that the direct
	// endpoint does, so we can forward the encoded body unchanged.
	if provider.browserPoolURL != "" {
		req.SetRequestURI(provider.browserPoolURL + "/chat")
		if streaming {
			req.Header.Set("Accept", "text/event-stream")
			req.Header.Set("Cache-Control", "no-cache")
		} else {
			req.Header.Set("Accept", "application/json, text/event-stream")
		}
		req.SetBody(body)
		return req
	}

	// Direct transport: hit chat.z.ai with reverse-engineered headers.
	req.SetRequestURI(provider.networkConfig.BaseURL + chatCompletionsPath)
	req.Header.Set("X-FE-Version", provider.feVersion)
	if streaming {
		req.Header.Set("Accept", "text/event-stream")
		req.Header.Set("Cache-Control", "no-cache")
	} else {
		req.Header.Set("Accept", "application/json, text/event-stream")
	}

	if v := key.Value.GetValue(); v != "" {
		req.Header.Set("Authorization", "Bearer "+v)
	}

	providerUtils.SetExtraHeaders(ctx, req, provider.networkConfig.ExtraHeaders, nil)
	req.SetBody(body)
	return req
}

// ----------------------------------------------------------------------------
// ChatCompletion (non-streaming): aggregate the SSE stream into a single response.
// ----------------------------------------------------------------------------

// ChatCompletion performs a chat completion request.
//
// Z.ai's web endpoint always responds with SSE (even for stream=false in the
// body, the server still chunks). We read the stream, accumulate all "answer"
// phase chunks (and apply edit_index/edit_content corrections), then return a
// single non-streaming BifrostChatResponse.
func (provider *ZaiWebProvider) ChatCompletion(ctx *schemas.BifrostContext, key schemas.Key, request *schemas.BifrostChatRequest) (*schemas.BifrostChatResponse, *schemas.BifrostError) {
	zaiReq, err := toZaiRequest(request, true) // always stream from upstream; we aggregate
	if err != nil {
		return nil, providerUtils.NewBifrostOperationError("failed to convert request", err)
	}

	jsonBody, err := sonic.Marshal(zaiReq)
	if err != nil {
		return nil, providerUtils.NewBifrostOperationError("failed to marshal request", err)
	}

	req := provider.buildHTTPRequest(ctx, jsonBody, key, true)
	defer fasthttp.ReleaseRequest(req)

	resp := fasthttp.AcquireResponse()
	resp.StreamBody = true

	if err := provider.streamingClient.Do(req, resp); err != nil {
		defer providerUtils.ReleaseStreamingResponse(resp)
		return nil, providerUtils.EnrichError(ctx,
			providerUtils.NewBifrostOperationError(schemas.ErrProviderDoRequest, err),
			jsonBody, nil, provider.sendBackRawRequest, provider.sendBackRawResponse)
	}

	if resp.StatusCode() != fasthttp.StatusOK {
		defer providerUtils.ReleaseStreamingResponse(resp)
		return nil, providerUtils.EnrichError(ctx, &schemas.BifrostError{
			IsBifrostError: false,
			StatusCode:     schemas.Ptr(resp.StatusCode()),
			Error: &schemas.ErrorField{
				Message: fmt.Sprintf("zai-web returned HTTP %d", resp.StatusCode()),
			},
		}, jsonBody, nil, provider.sendBackRawRequest, provider.sendBackRawResponse)
	}

	defer providerUtils.ReleaseStreamingResponse(resp)
	reader, releaseGzip := providerUtils.DecompressStreamBody(resp)
	defer releaseGzip()

	sseReader := providerUtils.GetSSEDataReader(ctx, reader)

	var (
		answer    strings.Builder
		reasoning strings.Builder
		usage     *zaiUsage
		zaiErr    *zaiError
		startedAt = time.Now()
	)

	for {
		data, readErr := sseReader.ReadDataLine()
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return nil, providerUtils.NewBifrostOperationError("failed to read sse stream", readErr)
		}
		var ev zaiSSEEvent
		if err := sonic.Unmarshal(data, &ev); err != nil {
			provider.logger.Warn("zaiweb: failed to parse sse chunk: %v", err)
			continue
		}
		applyChunk(&ev.Data, &answer, &reasoning, &usage, &zaiErr)
		if ev.Data.Phase == "done" {
			break
		}
	}

	if zaiErr != nil {
		return nil, &schemas.BifrostError{
			IsBifrostError: false,
			Error: &schemas.ErrorField{
				Message: fmt.Sprintf("zai-web error %d: %s", zaiErr.Code, zaiErr.Detail),
			},
		}
	}

	finalContent := answer.String()
	finalReasoning := reasoning.String()

	finishReason := string(schemas.BifrostFinishReasonStop)
	role := string(schemas.ChatMessageRoleAssistant)

	contentPtr := finalContent
	msg := &schemas.ChatMessage{
		Role:    schemas.ChatMessageRole(role),
		Content: &schemas.ChatMessageContent{ContentStr: &contentPtr},
	}

	resp_ := &schemas.BifrostChatResponse{
		ID:      generateChatCompletionID(),
		Object:  objectChatCompletion,
		Created: int(time.Now().Unix()),
		Model:   request.Model,
		Choices: []schemas.BifrostResponseChoice{
			{
				Index:        0,
				FinishReason: &finishReason,
				ChatNonStreamResponseChoice: &schemas.ChatNonStreamResponseChoice{
					Message: msg,
				},
			},
		},
	}
	if finalReasoning != "" && resp_.Choices[0].ChatNonStreamResponseChoice != nil {
		// Stash reasoning into ExtraParams so callers that care can pull it out;
		// the standard ChatMessage doesn't have a top-level reasoning field.
		if resp_.ExtraParams == nil {
			resp_.ExtraParams = make(map[string]interface{})
		}
		resp_.ExtraParams["reasoning"] = finalReasoning
	}
	if usage != nil {
		resp_.Usage = &schemas.BifrostLLMUsage{
			PromptTokens:     usage.PromptTokens,
			CompletionTokens: usage.CompletionTokens,
			TotalTokens:      usage.TotalTokens,
		}
	}
	resp_.ExtraFields.Latency = time.Since(startedAt).Milliseconds()
	return resp_, nil
}

// applyChunk accumulates a single SSE chunk's contribution to the buffered
// answer/reasoning/usage/error state. It also handles Z.ai's retroactive edit
// mechanism: when edit_index is set on a phase=other chunk, edit_content is
// inserted at that byte offset of the answer buffer.
func applyChunk(d *zaiSSEEvent_Data, answer, reasoning *strings.Builder, usage **zaiUsage, errOut **zaiError) {
	if d.Error != nil {
		*errOut = d.Error
		return
	}
	switch d.Phase {
	case "thinking":
		if d.DeltaContent != "" {
			reasoning.WriteString(d.DeltaContent)
		}
	case "answer":
		if d.DeltaContent != "" {
			answer.WriteString(d.DeltaContent)
		}
	case "other":
		if d.Usage != nil {
			*usage = d.Usage
		}
		if d.EditIndex != nil {
			// Splice edit_content into the buffered answer at the byte index.
			cur := answer.String()
			idx := *d.EditIndex
			if idx < 0 {
				idx = 0
			}
			if idx > len(cur) {
				idx = len(cur)
			}
			merged := cur[:idx] + d.EditContent + cur[idx:]
			answer.Reset()
			answer.WriteString(merged)
		}
	}
}

// ----------------------------------------------------------------------------
// ChatCompletionStream: forward Z.ai SSE as Bifrost stream chunks (OpenAI-shaped).
// ----------------------------------------------------------------------------

// ChatCompletionStream performs a streaming chat completion.
//
// Each Z.ai SSE chunk is converted in-flight to a BifrostChatResponse:
//   - phase=thinking -> Choices[0].Delta.Reasoning
//   - phase=answer   -> Choices[0].Delta.Content
//   - phase=other(usage) -> a chunk carrying usage but no choices
//   - phase=other(edit)  -> emitted as a delta whose content is edit_content,
//     since OpenAI's wire format has no rewind primitive.
//   - phase=done    -> finish_reason=stop on a final delta chunk.
func (provider *ZaiWebProvider) ChatCompletionStream(ctx *schemas.BifrostContext, postHookRunner schemas.PostHookRunner, postHookSpanFinalizer func(context.Context), key schemas.Key, request *schemas.BifrostChatRequest) (chan *schemas.BifrostStreamChunk, *schemas.BifrostError) {
	zaiReq, err := toZaiRequest(request, true)
	if err != nil {
		return nil, providerUtils.NewBifrostOperationError("failed to convert request", err)
	}

	jsonBody, err := sonic.Marshal(zaiReq)
	if err != nil {
		return nil, providerUtils.NewBifrostOperationError("failed to marshal request", err)
	}

	req := provider.buildHTTPRequest(ctx, jsonBody, key, true)
	defer fasthttp.ReleaseRequest(req)

	resp := fasthttp.AcquireResponse()
	resp.StreamBody = true

	if err := provider.streamingClient.Do(req, resp); err != nil {
		defer providerUtils.ReleaseStreamingResponse(resp)
		if errors.Is(err, context.Canceled) {
			return nil, providerUtils.EnrichError(ctx, &schemas.BifrostError{
				IsBifrostError: false,
				Error: &schemas.ErrorField{
					Type:    schemas.Ptr(schemas.RequestCancelled),
					Message: schemas.ErrRequestCancelled,
					Error:   err,
				},
			}, jsonBody, nil, provider.sendBackRawRequest, provider.sendBackRawResponse)
		}
		if errors.Is(err, fasthttp.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
			return nil, providerUtils.EnrichError(ctx, providerUtils.NewBifrostTimeoutError(schemas.ErrProviderRequestTimedOut, err), jsonBody, nil, provider.sendBackRawRequest, provider.sendBackRawResponse)
		}
		return nil, providerUtils.EnrichError(ctx, providerUtils.NewBifrostOperationError(schemas.ErrProviderDoRequest, err), jsonBody, nil, provider.sendBackRawRequest, provider.sendBackRawResponse)
	}

	ctx.SetValue(schemas.BifrostContextKeyProviderResponseHeaders, providerUtils.ExtractProviderResponseHeaders(resp))

	if resp.StatusCode() != fasthttp.StatusOK {
		defer providerUtils.ReleaseStreamingResponse(resp)
		return nil, providerUtils.EnrichError(ctx, &schemas.BifrostError{
			IsBifrostError: false,
			StatusCode:     schemas.Ptr(resp.StatusCode()),
			Error: &schemas.ErrorField{
				Message: fmt.Sprintf("zai-web returned HTTP %d", resp.StatusCode()),
			},
		}, jsonBody, nil, provider.sendBackRawRequest, provider.sendBackRawResponse)
	}

	if providerUtils.SetupStreamingPassthrough(ctx, resp) {
		responseChan := make(chan *schemas.BifrostStreamChunk)
		close(responseChan)
		return responseChan, nil
	}

	responseChan := make(chan *schemas.BifrostStreamChunk, schemas.DefaultStreamBufferSize)
	providerUtils.SetStreamIdleTimeoutIfEmpty(ctx, provider.networkConfig.StreamIdleTimeoutInSeconds)

	responseID := generateChatCompletionID()
	model := request.Model

	go func() {
		defer providerUtils.EnsureStreamFinalizerCalled(ctx, postHookSpanFinalizer)
		defer func() {
			if ctx.Err() == context.Canceled {
				providerUtils.HandleStreamCancellation(ctx, postHookRunner, responseChan, provider.logger, postHookSpanFinalizer, jsonBody)
			} else if ctx.Err() == context.DeadlineExceeded {
				providerUtils.HandleStreamTimeout(ctx, postHookRunner, responseChan, provider.logger, postHookSpanFinalizer, jsonBody)
			}
			close(responseChan)
		}()
		defer providerUtils.ReleaseStreamingResponse(resp)

		reader, releaseGzip := providerUtils.DecompressStreamBody(resp)
		defer releaseGzip()

		reader, stopIdleTimeout := providerUtils.NewIdleTimeoutReader(reader, resp.BodyStream(), providerUtils.GetStreamIdleTimeout(ctx))
		defer stopIdleTimeout()

		stopCancellation := providerUtils.SetupStreamCancellation(ctx, resp.BodyStream(), provider.logger)
		defer stopCancellation()

		sseReader := providerUtils.GetSSEDataReader(ctx, reader)

		startTime := time.Now()
		lastChunkTime := startTime
		chunkIndex := 0

		emit := func(chatResp *schemas.BifrostChatResponse) {
			if chatResp == nil {
				return
			}
			chatResp.ID = responseID
			chatResp.Model = model
			chatResp.Object = objectChatCompletionChunk
			chatResp.Created = int(time.Now().Unix())
			chatResp.ExtraFields.ChunkIndex = chunkIndex
			chatResp.ExtraFields.Latency = time.Since(lastChunkTime).Milliseconds()
			chunkIndex++
			lastChunkTime = time.Now()
			providerUtils.ProcessAndSendResponse(
				ctx, postHookRunner,
				providerUtils.GetBifrostResponseForStreamResponse(nil, chatResp, nil, nil, nil, nil),
				responseChan, postHookSpanFinalizer,
			)
		}

		for {
			if ctx.Err() != nil {
				return
			}
			data, readErr := sseReader.ReadDataLine()
			if readErr != nil {
				if readErr != io.EOF {
					ctx.SetValue(schemas.BifrostContextKeyStreamEndIndicator, true)
					provider.logger.Warn("zaiweb: error reading stream: %v", readErr)
					providerUtils.ProcessAndSendError(ctx, postHookRunner, readErr, responseChan, provider.logger, postHookSpanFinalizer)
					return
				}
				break
			}

			var ev zaiSSEEvent
			if err := sonic.Unmarshal(data, &ev); err != nil {
				provider.logger.Warn("zaiweb: bad sse chunk: %v", err)
				continue
			}

			d := ev.Data
			if d.Error != nil {
				ctx.SetValue(schemas.BifrostContextKeyStreamEndIndicator, true)
				bifrostErr := &schemas.BifrostError{
					IsBifrostError: false,
					Error: &schemas.ErrorField{
						Message: fmt.Sprintf("zai-web error %d: %s", d.Error.Code, d.Error.Detail),
					},
				}
				providerUtils.ProcessAndSendBifrostError(ctx, postHookRunner, bifrostErr, responseChan, provider.logger, postHookSpanFinalizer)
				return
			}

			switch d.Phase {
			case "thinking":
				if d.DeltaContent == "" {
					continue
				}
				reasoning := d.DeltaContent
				emit(&schemas.BifrostChatResponse{
					Choices: []schemas.BifrostResponseChoice{
						{
							Index: 0,
							ChatStreamResponseChoice: &schemas.ChatStreamResponseChoice{
								Delta: &schemas.ChatStreamResponseChoiceDelta{Reasoning: &reasoning},
							},
						},
					},
				})

			case "answer":
				if d.DeltaContent == "" {
					continue
				}
				content := d.DeltaContent
				emit(&schemas.BifrostChatResponse{
					Choices: []schemas.BifrostResponseChoice{
						{
							Index: 0,
							ChatStreamResponseChoice: &schemas.ChatStreamResponseChoice{
								Delta: &schemas.ChatStreamResponseChoiceDelta{Content: &content},
							},
						},
					},
				})

			case "other":
				if d.Usage != nil {
					emit(&schemas.BifrostChatResponse{
						Usage: &schemas.BifrostLLMUsage{
							PromptTokens:     d.Usage.PromptTokens,
							CompletionTokens: d.Usage.CompletionTokens,
							TotalTokens:      d.Usage.TotalTokens,
						},
					})
				}
				if d.EditIndex != nil && d.EditContent != "" {
					// OpenAI streaming has no edit primitive; the cleanest
					// approximation is to emit the edit_content as an extra
					// content delta. Clients that re-render from the full
					// stream will see the appended/inserted text at the end.
					content := d.EditContent
					emit(&schemas.BifrostChatResponse{
						Choices: []schemas.BifrostResponseChoice{
							{
								Index: 0,
								ChatStreamResponseChoice: &schemas.ChatStreamResponseChoice{
									Delta: &schemas.ChatStreamResponseChoiceDelta{Content: &content},
								},
							},
						},
					})
				}

			case "done":
				stop := string(schemas.BifrostFinishReasonStop)
				ctx.SetValue(schemas.BifrostContextKeyStreamEndIndicator, true)
				final := &schemas.BifrostChatResponse{
					Choices: []schemas.BifrostResponseChoice{
						{
							Index:        0,
							FinishReason: &stop,
							ChatStreamResponseChoice: &schemas.ChatStreamResponseChoice{
								Delta: &schemas.ChatStreamResponseChoiceDelta{},
							},
						},
					},
				}
				final.ExtraFields.Latency = time.Since(startTime).Milliseconds()
				emit(final)
				return
			}
		}
	}()

	return responseChan, nil
}

// ----------------------------------------------------------------------------
// Unsupported operations.
//
// The web endpoint only exposes chat completions. Everything else returns
// an UnsupportedOperationError so the gateway can fall back gracefully.
// ----------------------------------------------------------------------------

// ListModels is not supported by the ZaiWeb provider. The web API does not
// expose a list models endpoint accessible to anonymous-style web sessions.
func (provider *ZaiWebProvider) ListModels(ctx *schemas.BifrostContext, keys []schemas.Key, request *schemas.BifrostListModelsRequest) (*schemas.BifrostListModelsResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ListModelsRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) TextCompletion(ctx *schemas.BifrostContext, key schemas.Key, request *schemas.BifrostTextCompletionRequest) (*schemas.BifrostTextCompletionResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError("text completion", "zai-web")
}

func (provider *ZaiWebProvider) TextCompletionStream(ctx *schemas.BifrostContext, postHookRunner schemas.PostHookRunner, postHookSpanFinalizer func(context.Context), key schemas.Key, request *schemas.BifrostTextCompletionRequest) (chan *schemas.BifrostStreamChunk, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError("text completion", "zai-web")
}

func (provider *ZaiWebProvider) Responses(ctx *schemas.BifrostContext, key schemas.Key, request *schemas.BifrostResponsesRequest) (*schemas.BifrostResponsesResponse, *schemas.BifrostError) {
	chatResponse, err := provider.ChatCompletion(ctx, key, request.ToChatRequest())
	if err != nil {
		return nil, err
	}
	return chatResponse.ToBifrostResponsesResponse(), nil
}

func (provider *ZaiWebProvider) ResponsesStream(ctx *schemas.BifrostContext, postHookRunner schemas.PostHookRunner, postHookSpanFinalizer func(context.Context), key schemas.Key, request *schemas.BifrostResponsesRequest) (chan *schemas.BifrostStreamChunk, *schemas.BifrostError) {
	ctx.SetValue(schemas.BifrostContextKeyIsResponsesToChatCompletionFallback, true)
	return provider.ChatCompletionStream(ctx, postHookRunner, postHookSpanFinalizer, key, request.ToChatRequest())
}

func (provider *ZaiWebProvider) Embedding(ctx *schemas.BifrostContext, key schemas.Key, request *schemas.BifrostEmbeddingRequest) (*schemas.BifrostEmbeddingResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.EmbeddingRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) Speech(ctx *schemas.BifrostContext, key schemas.Key, request *schemas.BifrostSpeechRequest) (*schemas.BifrostSpeechResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.SpeechRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) SpeechStream(ctx *schemas.BifrostContext, postHookRunner schemas.PostHookRunner, postHookSpanFinalizer func(context.Context), key schemas.Key, request *schemas.BifrostSpeechRequest) (chan *schemas.BifrostStreamChunk, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.SpeechStreamRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) Transcription(ctx *schemas.BifrostContext, key schemas.Key, request *schemas.BifrostTranscriptionRequest) (*schemas.BifrostTranscriptionResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.TranscriptionRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) TranscriptionStream(ctx *schemas.BifrostContext, postHookRunner schemas.PostHookRunner, postHookSpanFinalizer func(context.Context), key schemas.Key, request *schemas.BifrostTranscriptionRequest) (chan *schemas.BifrostStreamChunk, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.TranscriptionStreamRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) Rerank(ctx *schemas.BifrostContext, key schemas.Key, request *schemas.BifrostRerankRequest) (*schemas.BifrostRerankResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.RerankRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) OCR(ctx *schemas.BifrostContext, key schemas.Key, request *schemas.BifrostOCRRequest) (*schemas.BifrostOCRResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.OCRRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) ImageGeneration(ctx *schemas.BifrostContext, key schemas.Key, request *schemas.BifrostImageGenerationRequest) (*schemas.BifrostImageGenerationResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ImageGenerationRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) ImageGenerationStream(ctx *schemas.BifrostContext, postHookRunner schemas.PostHookRunner, postHookSpanFinalizer func(context.Context), key schemas.Key, request *schemas.BifrostImageGenerationRequest) (chan *schemas.BifrostStreamChunk, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ImageGenerationStreamRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) ImageEdit(ctx *schemas.BifrostContext, key schemas.Key, request *schemas.BifrostImageEditRequest) (*schemas.BifrostImageGenerationResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ImageEditRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) ImageEditStream(ctx *schemas.BifrostContext, postHookRunner schemas.PostHookRunner, postHookSpanFinalizer func(context.Context), key schemas.Key, request *schemas.BifrostImageEditRequest) (chan *schemas.BifrostStreamChunk, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ImageEditStreamRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) ImageVariation(ctx *schemas.BifrostContext, key schemas.Key, request *schemas.BifrostImageVariationRequest) (*schemas.BifrostImageGenerationResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ImageVariationRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) VideoGeneration(_ *schemas.BifrostContext, _ schemas.Key, _ *schemas.BifrostVideoGenerationRequest) (*schemas.BifrostVideoGenerationResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.VideoGenerationRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) VideoRetrieve(_ *schemas.BifrostContext, _ schemas.Key, _ *schemas.BifrostVideoRetrieveRequest) (*schemas.BifrostVideoGenerationResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.VideoRetrieveRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) VideoDownload(_ *schemas.BifrostContext, _ schemas.Key, _ *schemas.BifrostVideoDownloadRequest) (*schemas.BifrostVideoDownloadResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.VideoDownloadRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) VideoDelete(_ *schemas.BifrostContext, _ schemas.Key, _ *schemas.BifrostVideoDeleteRequest) (*schemas.BifrostVideoDeleteResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.VideoDeleteRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) VideoList(_ *schemas.BifrostContext, _ schemas.Key, _ *schemas.BifrostVideoListRequest) (*schemas.BifrostVideoListResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.VideoListRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) VideoRemix(_ *schemas.BifrostContext, _ schemas.Key, _ *schemas.BifrostVideoRemixRequest) (*schemas.BifrostVideoGenerationResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.VideoRemixRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) BatchCreate(_ *schemas.BifrostContext, _ schemas.Key, _ *schemas.BifrostBatchCreateRequest) (*schemas.BifrostBatchCreateResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.BatchCreateRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) BatchList(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostBatchListRequest) (*schemas.BifrostBatchListResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.BatchListRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) BatchRetrieve(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostBatchRetrieveRequest) (*schemas.BifrostBatchRetrieveResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.BatchRetrieveRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) BatchCancel(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostBatchCancelRequest) (*schemas.BifrostBatchCancelResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.BatchCancelRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) BatchDelete(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostBatchDeleteRequest) (*schemas.BifrostBatchDeleteResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.BatchDeleteRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) BatchResults(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostBatchResultsRequest) (*schemas.BifrostBatchResultsResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.BatchResultsRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) FileUpload(_ *schemas.BifrostContext, _ schemas.Key, _ *schemas.BifrostFileUploadRequest) (*schemas.BifrostFileUploadResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.FileUploadRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) FileList(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostFileListRequest) (*schemas.BifrostFileListResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.FileListRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) FileRetrieve(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostFileRetrieveRequest) (*schemas.BifrostFileRetrieveResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.FileRetrieveRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) FileDelete(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostFileDeleteRequest) (*schemas.BifrostFileDeleteResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.FileDeleteRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) FileContent(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostFileContentRequest) (*schemas.BifrostFileContentResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.FileContentRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) CountTokens(_ *schemas.BifrostContext, _ schemas.Key, _ *schemas.BifrostResponsesRequest) (*schemas.BifrostCountTokensResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.CountTokensRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) ContainerCreate(_ *schemas.BifrostContext, _ schemas.Key, _ *schemas.BifrostContainerCreateRequest) (*schemas.BifrostContainerCreateResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ContainerCreateRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) ContainerList(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostContainerListRequest) (*schemas.BifrostContainerListResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ContainerListRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) ContainerRetrieve(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostContainerRetrieveRequest) (*schemas.BifrostContainerRetrieveResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ContainerRetrieveRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) ContainerDelete(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostContainerDeleteRequest) (*schemas.BifrostContainerDeleteResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ContainerDeleteRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) ContainerFileCreate(_ *schemas.BifrostContext, _ schemas.Key, _ *schemas.BifrostContainerFileCreateRequest) (*schemas.BifrostContainerFileCreateResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ContainerFileCreateRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) ContainerFileList(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostContainerFileListRequest) (*schemas.BifrostContainerFileListResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ContainerFileListRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) ContainerFileRetrieve(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostContainerFileRetrieveRequest) (*schemas.BifrostContainerFileRetrieveResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ContainerFileRetrieveRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) ContainerFileContent(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostContainerFileContentRequest) (*schemas.BifrostContainerFileContentResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ContainerFileContentRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) ContainerFileDelete(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostContainerFileDeleteRequest) (*schemas.BifrostContainerFileDeleteResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.ContainerFileDeleteRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) CachedContentCreate(_ *schemas.BifrostContext, _ schemas.Key, _ *schemas.BifrostCachedContentCreateRequest) (*schemas.BifrostCachedContentCreateResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.CachedContentCreateRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) CachedContentList(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostCachedContentListRequest) (*schemas.BifrostCachedContentListResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.CachedContentListRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) CachedContentRetrieve(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostCachedContentRetrieveRequest) (*schemas.BifrostCachedContentRetrieveResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.CachedContentRetrieveRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) CachedContentUpdate(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostCachedContentUpdateRequest) (*schemas.BifrostCachedContentUpdateResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.CachedContentUpdateRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) CachedContentDelete(_ *schemas.BifrostContext, _ []schemas.Key, _ *schemas.BifrostCachedContentDeleteRequest) (*schemas.BifrostCachedContentDeleteResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.CachedContentDeleteRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) Passthrough(_ *schemas.BifrostContext, _ schemas.Key, _ *schemas.BifrostPassthroughRequest) (*schemas.BifrostPassthroughResponse, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.PassthroughRequest, schemas.ZaiWeb)
}

func (provider *ZaiWebProvider) PassthroughStream(_ *schemas.BifrostContext, _ schemas.PostHookRunner, _ func(context.Context), _ schemas.Key, _ *schemas.BifrostPassthroughRequest) (chan *schemas.BifrostStreamChunk, *schemas.BifrostError) {
	return nil, providerUtils.NewUnsupportedOperationError(schemas.PassthroughStreamRequest, schemas.ZaiWeb)
}
