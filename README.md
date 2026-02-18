# Zulip2Telegram

A robust bridge application written in Go that forwards messages from Zulip streams to a Telegram channel.

## Features

*   **Real-time Forwarding**: Uses Zulip's real-time event queue to forward messages instantly.
*   **Attachment Support**: Automatically downloads and forwards images and documents (up to Telegram's size limits).
*   **Smart Filtering**: Configure specific Zulip streams or topics to forward.
*   **Rate Limiting**: Built-in rate limiter with exponential backoff to comply with Telegram API limits.
*   **Markdown Support**: Preserves basic formatting where possible.

## Prerequisites

*   Go 1.21+
*   A Zulip Bot account (Generic Bot)
*   A Telegram Bot (created via @BotFather)
*   A Telegram Channel (add your bot as an administrator)

## Installation

1.  Clone the repository:
    ```bash
    git clone github.com/khaliullov/zulip2tg
    cd zulip2tg
    ```

2.  Build the binary:
    ```bash
    go build -o zulip2tg .
    ```

## Configuration

1.  Create a configuration file based on the example:
    ```bash
    cp config.yaml.example config.yaml
    ```

2.  Edit `config.yaml` and fill in your credentials:

    ```yaml
    zulip:
      site: "https://zulip.example.com"
      email: "bot@zulip.example.com"
      key: "your-zulip-api-key"

    telegram:
      bot_token: "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
      channel_id: "-1001234567890"
    ```

3.  (Optional) Configure filters to only forward specific content:
    ```yaml
    filters:
      streams: ["announce", "general"]
      topics: ["releases"]
    ```

## Usage

Run the bridge by specifying the path to your configuration file:

```bash
./zulip2tg -c config.yaml
```

The bot will start listening for new messages on Zulip and forward them to the configured Telegram channel.

## Deploy on Debian

Create /etc/systemd/system/zulip2tg.service

    [Unit]
    Description=Zulip to Telegram Bridge
    After=network.target
    
    [Service]
    Type=simple
    User=zulip2tg
    ExecStart=/opt/zulip2tg/zulip2tg -c /etc/zulip2tg/config.yaml
    Restart=on-failure
    RestartSec=10
    
    [Install]
    WantedBy=multi-user.target

