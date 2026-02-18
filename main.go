package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/wakumaku/go-zulip"
	"github.com/wakumaku/go-zulip/realtime"
	"github.com/wakumaku/go-zulip/realtime/events"
	"golang.org/x/time/rate"
)

const (
	defaultConfigPath = "/etc/zulip2tg/config.yaml"
	maxMessageLength  = 4096
	telegramMaxPhoto  = 20 * 1024 * 1024
	telegramMaxDoc    = 50 * 1024 * 1024
)

// QueuedMessage represents a message waiting to be sent
type QueuedMessage struct {
	Text        string
	Attachments []*Attachment
	Priority    int
}

// Attachment represents a file attachment from Zulip
type Attachment struct {
	URL      string
	Filename string
	Size     int64
	MimeType string
}

type Bot struct {
	cfg          *Config
	zulipCli     *zulip.Client
	tgBot        *tgbotapi.BotAPI
	channelID    int64
	logger       *slog.Logger
	email        string
	rateLimiter  *rate.Limiter
	httpClient   *http.Client
	queue        chan *QueuedMessage
	queueWg      sync.WaitGroup
	shutdownChan chan struct{}
}

func main() {
	configPath := flag.String("c", defaultConfigPath, "Path to configuration YAML file")
	flag.Parse()

	cfg, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	bot, err := NewBot(cfg, logger)
	if err != nil {
		log.Fatalf("Failed to initialize bot: %v", err)
	}

	logger.Info("Starting Zulip2Telegram bridge", "config", *configPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		logger.Info("Shutdown signal received, stopping...")
		cancel()
		bot.Shutdown()
	}()

	if err := bot.Run(ctx); err != nil {
		log.Fatalf("Bot runtime error: %v", err)
	}
}

func NewBot(cfg *Config, logger *slog.Logger) (*Bot, error) {
	creds := zulip.Credentials(cfg.Zulip.Site, cfg.Zulip.Email, cfg.Zulip.Key)
	zulipCli, err := zulip.NewClient(creds, zulip.WithLogger(logger))
	if err != nil {
		return nil, fmt.Errorf("failed to create Zulip client: %w", err)
	}

	tgBot, err := tgbotapi.NewBotAPI(cfg.Telegram.BotToken)
	if err != nil {
		return nil, fmt.Errorf("failed to create Telegram bot: %w", err)
	}
	tgBot.Debug = false

	// Parse channel ID (numeric only for simplicity)
	channelID, err := parseChannelID(cfg.Telegram.ChannelID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse channel ID: %w", err)
	}

	// Initialize rate limiter
	rateLimiter := rate.NewLimiter(
		rate.Limit(cfg.RateLimit.MessagesPerSecond),
		cfg.RateLimit.Burst,
	)

	// Initialize HTTP client for attachment downloads
	httpClient := &http.Client{
		Timeout: cfg.Attachments.DownloadTimeout,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	// Initialize message queue (buffered)
	queue := make(chan *QueuedMessage, 100)

	logger.Info("Bot initialized",
		"zulip_site", cfg.Zulip.Site,
		"telegram_channel", cfg.Telegram.ChannelID,
		"channel_id", channelID,
		"rate_limit", cfg.RateLimit.MessagesPerSecond,
		"attachments_enabled", cfg.Attachments.Enabled,
	)

	bot := &Bot{
		cfg:          cfg,
		zulipCli:     zulipCli,
		tgBot:        tgBot,
		channelID:    channelID,
		logger:       logger,
		email:        cfg.Zulip.Email,
		rateLimiter:  rateLimiter,
		httpClient:   httpClient,
		queue:        queue,
		shutdownChan: make(chan struct{}),
	}

	// Start queue processor
	bot.startQueueProcessor()

	return bot, nil
}

// startQueueProcessor starts the background goroutine that processes the message queue
func (b *Bot) startQueueProcessor() {
	b.queueWg.Add(1)
	go func() {
		defer b.queueWg.Done()
		for {
			select {
			case <-b.shutdownChan:
				b.logger.Info("Queue processor shutting down")
				// Drain remaining messages
				for len(b.queue) > 0 {
					msg := <-b.queue
					b.sendWithRetry(msg)
				}
				return
			case msg := <-b.queue:
				if err := b.rateLimiter.Wait(context.Background()); err != nil {
					b.logger.Error("Rate limiter error", "error", err)
					continue
				}
				b.sendWithRetry(msg)
			}
		}
	}()
}

// Shutdown gracefully stops the bot
func (b *Bot) Shutdown() {
	close(b.shutdownChan)
	b.queueWg.Wait()
	b.logger.Info("Bot shutdown complete")
}

// parseChannelID converts string channel ID to int64
func parseChannelID(identifier string) (int64, error) {
	var id int64
	_, err := fmt.Sscanf(identifier, "%d", &id)
	if err != nil {
		return 0, fmt.Errorf("channel_id must be numeric (e.g., -1001234567890), got: %s", identifier)
	}
	return id, nil
}

func (b *Bot) Run(ctx context.Context) error {
	b.queueMessage(&QueuedMessage{
		Text: "🔄 Zulip2Telegram bridge started",
	})

	realtimeSvc := realtime.NewService(b.zulipCli)

	const maxBackoff = 30 * time.Second
	backoff := 1 * time.Second

	for {
		select {
		case <-ctx.Done():
			b.logger.Info("Context cancelled, shutting down")
			return nil
		default:
		}

		if backoff > 1*time.Second {
			b.logger.Info("Reconnecting", "in", backoff)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return nil
			}
		}

		b.logger.Info("Registering event queue")
		queue, err := realtimeSvc.RegisterEvetQueue(ctx,
			realtime.EventTypes(events.MessageType),
			realtime.AllPublicStreams(true),
			realtime.ApplyMarkdown(true),
		)
		if err != nil {
			b.logger.Error("Failed to register event queue", "error", err)
			backoff = minDuration(backoff*2, maxBackoff)
			continue
		}

		if queue.IsError() {
			b.logger.Error("Queue registration error", "msg", queue.Msg(), "code", queue.Code())
			backoff = minDuration(backoff*2, maxBackoff)
			continue
		}

		b.logger.Info("Event queue registered", "queue_id", queue.QueueID, "last_event_id", queue.LastEventID)
		backoff = 1 * time.Second
		lastEventID := queue.LastEventID

		for {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			ctxPoll, cancel := context.WithTimeout(ctx, 70*time.Second)
			eventBatch, err := realtimeSvc.GetEventsEventQueue(ctxPoll, queue.QueueID, realtime.LastEventID(lastEventID))
			cancel()

			if err != nil {
				b.logger.Error("Error fetching events", "error", err)
				break
			}

			for _, evt := range eventBatch.Events {
				lastEventID = evt.EventID()

				if msgEvt, ok := evt.(*events.Message); ok {
					b.handleZulipMessage(ctx, msgEvt)
				}
			}
		}
	}
}

func (b *Bot) handleZulipMessage(_ context.Context, evt *events.Message) {
	// Skip self-messages
	if evt.Message.SenderEmail == b.email {
		return
	}

	// Apply stream filter
	if len(b.cfg.Filters.Streams) > 0 {
		matched := false
		streamName := evt.Message.DisplayRecipient.Channel
		for _, stream := range b.cfg.Filters.Streams {
			if strings.EqualFold(streamName, stream) {
				matched = true
				break
			}
		}
		if matched {
			return
		}
	}

	// Apply topic filter
	if len(b.cfg.Filters.Topics) > 0 {
		matched := false
		for _, topic := range b.cfg.Filters.Topics {
			if strings.EqualFold(evt.Message.Subject, topic) {
				matched = true
				break
			}
		}
		if matched {
			return
		}
	}

	// Format message with optional Zulip link
	formattedMsg := formatZulipMessage(evt, b.cfg.Zulip.Site, b.cfg.MessageFormat.IncludeZulipLink)

	// Process attachments if enabled
	var attachments []*Attachment
	if b.cfg.Attachments.Enabled {
		attachments = b.processAttachments(evt)
	}

	// Queue message for sending
	b.queueMessage(&QueuedMessage{
		Text:        formattedMsg,
		Attachments: attachments,
		Priority:    0,
	})
}

// processAttachments extracts attachment info from Zulip message
// NOTE: Field names depend on go-zulip library version
func (b *Bot) processAttachments(evt *events.Message) []*Attachment {
	var attachments []*Attachment

	// Try different possible attachment field locations
	// The go-zulip library structure may vary

	// Option 1: Check Message.Attachments
	// Option 2: Check Message.Content for embedded links
	// Option 3: Check Message.Meta or similar

	// For now, we'll parse the message content for common attachment patterns
	// This is a fallback - adjust based on your actual library structure

	content := evt.Message.Content

	// Look for file links in content (Zulip typically embeds these)
	// Example: <a href="/user_uploads/2/...">filename.pdf</a>
	if strings.Contains(content, "/user_uploads/") {
		// Extract upload URLs from content
		uploads := extractUploadLinks(content, b.cfg.Zulip.Site)
		for _, upload := range uploads {
			attachments = append(attachments, &Attachment{
				URL:      upload.URL,
				Filename: upload.Filename,
				Size:     0, // Size not available from content parsing
				MimeType: "",
			})
		}
	}

	// Log attachment discovery for debugging
	if len(attachments) > 0 {
		b.logger.Debug("Found attachments", "count", len(attachments))
	}

	return attachments
}

// UploadedFile represents an extracted file link
type UploadedFile struct {
	URL      string
	Filename string
}

// extractUploadLinks parses Zulip message content for file upload links
func extractUploadLinks(content, zulipSite string) []UploadedFile {
	var files []UploadedFile

	// Simple extraction of /user_uploads/ links
	// For production, use proper HTML parser (golang.org/x/net/html)
	startIdx := 0
	for {
		idx := strings.Index(content[startIdx:], "/user_uploads/")
		if idx == -1 {
			break
		}
		idx += startIdx

		// Find the end of the URL (space, quote, or end of string)
		endIdx := idx
		for endIdx < len(content) {
			c := content[endIdx]
			if c == ' ' || c == '"' || c == '\'' || c == '<' || c == '>' {
				break
			}
			endIdx++
		}

		urlPath := content[idx:endIdx]
		fullURL := zulipSite + urlPath

		// Try to extract filename from URL or nearby text
		filename := "attachment"
		if slashIdx := strings.LastIndex(urlPath, "/"); slashIdx != -1 {
			filename = urlPath[slashIdx+1:]
		}

		files = append(files, UploadedFile{
			URL:      fullURL,
			Filename: filename,
		})

		startIdx = endIdx
	}

	return files
}

// queueMessage adds a message to the send queue
func (b *Bot) queueMessage(msg *QueuedMessage) {
	select {
	case b.queue <- msg:
		b.logger.Debug("Message queued", "text_len", len(msg.Text), "attachments", len(msg.Attachments))
	default:
		b.logger.Warn("Queue full, dropping message")
	}
}

// sendWithRetry sends a message with exponential backoff retry logic
func (b *Bot) sendWithRetry(msg *QueuedMessage) {
	var lastErr error

	for attempt := 0; attempt <= b.cfg.RateLimit.MaxRetries; attempt++ {
		if attempt > 0 {
			backoff := b.calculateBackoff(attempt)
			b.logger.Info("Retrying message send", "attempt", attempt, "backoff", backoff)
			time.Sleep(backoff)
		}

		if err := b.sendMessage(msg); err != nil {
			lastErr = err
			b.logger.Error("Send failed", "attempt", attempt+1, "error", err)

			if !b.isRetryableError(err) {
				b.logger.Error("Non-retryable error, giving up", "error", err)
				return
			}
			continue
		}

		b.logger.Debug("Message sent successfully")
		return
	}

	b.logger.Error("Failed to send message after all retries", "error", lastErr)
}

// calculateBackoff calculates exponential backoff with jitter
func (b *Bot) calculateBackoff(attempt int) time.Duration {
	base := b.cfg.RateLimit.InitialBackoff
	max := b.cfg.RateLimit.MaxBackoff

	backoff := base * time.Duration(1<<uint(attempt-1))
	if backoff > max {
		backoff = max
	}

	jitter := time.Duration(float64(backoff) * 0.25 * (float64(time.Now().UnixNano()%1000)/1000.0 - 0.5))
	return backoff + jitter
}

// isRetryableError determines if an error is worth retrying
func (b *Bot) isRetryableError(err error) bool {
	errStr := err.Error()

	if strings.Contains(errStr, "Too Many Requests") || strings.Contains(errStr, "429") {
		return true
	}

	if strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "connection") ||
		strings.Contains(errStr, "network") {
		return true
	}

	if strings.Contains(errStr, "500") ||
		strings.Contains(errStr, "502") ||
		strings.Contains(errStr, "503") ||
		strings.Contains(errStr, "504") {
		return true
	}

	return false
}

// sendMessage sends a message with attachments to Telegram
func (b *Bot) sendMessage(msg *QueuedMessage) error {
	if msg.Text != "" {
		if err := b.sendTextMessage(msg.Text); err != nil {
			return fmt.Errorf("failed to send text: %w", err)
		}
	}

	for _, att := range msg.Attachments {
		if err := b.sendAttachment(att); err != nil {
			b.logger.Warn("Failed to send attachment", "filename", att.Filename, "error", err)
		}
	}

	return nil
}

// sendTextMessage sends a text message to Telegram
func (b *Bot) sendTextMessage(text string) error {
	if len(text) > maxMessageLength {
		text = text[:maxMessageLength-100] + "\n...(truncated)"
	}

	msg := tgbotapi.NewMessage(b.channelID, text)
	msg.ParseMode = tgbotapi.ModeMarkdownV2
	msg.DisableWebPagePreview = false

	_, err := b.tgBot.Send(msg)
	return err
}

// sendAttachment downloads and sends an attachment to Telegram
func (b *Bot) sendAttachment(att *Attachment) error {
	data, err := b.downloadFile(att.URL)
	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}

	isPhoto := strings.HasPrefix(att.MimeType, "image/") ||
		strings.HasSuffix(strings.ToLower(att.Filename), ".jpg") ||
		strings.HasSuffix(strings.ToLower(att.Filename), ".jpeg") ||
		strings.HasSuffix(strings.ToLower(att.Filename), ".png") ||
		strings.HasSuffix(strings.ToLower(att.Filename), ".gif")

	var sendErr error
	if isPhoto && len(data) <= telegramMaxPhoto {
		photo := tgbotapi.NewPhoto(b.channelID, tgbotapi.FileBytes{
			Name:  att.Filename,
			Bytes: data,
		})
		photo.Caption = fmt.Sprintf("📎 %s", att.Filename)
		photo.ParseMode = tgbotapi.ModeHTML
		_, sendErr = b.tgBot.Send(photo)
	} else {
		maxSize := telegramMaxDoc
		if len(data) > maxSize {
			return fmt.Errorf("file too large: %d bytes (max: %d)", len(data), maxSize)
		}
		doc := tgbotapi.NewDocument(b.channelID, tgbotapi.FileBytes{
			Name:  att.Filename,
			Bytes: data,
		})
		doc.Caption = fmt.Sprintf("📎 %s (%s)", att.Filename, formatFileSize(int64(len(data))))
		doc.ParseMode = tgbotapi.ModeHTML
		_, sendErr = b.tgBot.Send(doc)
	}

	return sendErr
}

// downloadFile downloads a file from URL with authentication for Zulip
func (b *Bot) downloadFile(url string) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(b.cfg.Zulip.Email, b.cfg.Zulip.Key)

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	maxSize := int64(b.cfg.Attachments.MaxSizeMB) * 1024 * 1024
	if resp.ContentLength > maxSize && resp.ContentLength > 0 {
		return nil, fmt.Errorf("file too large: %d bytes", resp.ContentLength)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxSize+1))
	if err != nil {
		return nil, err
	}

	if int64(len(data)) > maxSize {
		return nil, fmt.Errorf("file too large: %d bytes", len(data))
	}

	return data, nil
}

func formatZulipMessage(evt *events.Message, zulipSite string, includeLink bool) string {
	var sb strings.Builder

	if evt.Message.DisplayRecipient.IsChannel {
		sb.WriteString(fmt.Sprintf("📢 *%s* ▸ *%s*\n",
			escapeMarkdown(evt.Message.DisplayRecipient.Channel),
			escapeMarkdown(evt.Message.Subject),
		))
	} else {
		sb.WriteString("🔐 *Private Message*\n")
	}

	sb.WriteString(fmt.Sprintf("👤 *%s*:\n", escapeMarkdown(evt.Message.SenderFullName)))

	content := stripZulipFormatting(evt.Message.Content)
	sb.WriteString(escapeMarkdown(content))

	// Add Zulip message link only if enabled in config
	if includeLink && evt.Message.ID != 0 && evt.Message.DisplayRecipient.IsChannel {
		stream := evt.Message.DisplayRecipient.Channel
		topic := evt.Message.Subject
		msgURL := fmt.Sprintf("%s/#narrow/channel/%s/topic/%s/near/%d",
			zulipSite,
			stream,
			topic,
			evt.Message.ID,
		)
		sb.WriteString(fmt.Sprintf("\n\n[🔗 View in Zulip](%s)", escapeMarkdown(msgURL)))
	}

	return sb.String()
}

func stripZulipFormatting(content string) string {
	result := content

	for strings.Contains(result, "<") && strings.Contains(result, ">") {
		start := strings.Index(result, "<")
		end := strings.Index(result[start:], ">")
		if end == -1 {
			break
		}
		result = result[:start] + result[start+end+1:]
	}

	result = strings.ReplaceAll(result, "@**", "@")
	result = strings.ReplaceAll(result, "**", "")
	result = strings.ReplaceAll(result, "```", "`")

	return strings.TrimSpace(result)
}

func escapeMarkdown(s string) string {
	escapes := []string{"_", "*", "[", "]", "(", ")", "~", "`", ">", "#", "+", "-", "=", "|", "{", "}", ".", "!"}
	result := s
	for _, char := range escapes {
		result = strings.ReplaceAll(result, char, "\\"+char)
	}
	return result
}

func formatFileSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
