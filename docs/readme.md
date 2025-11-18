# IB API Realtime Server – WebSocket Market Data Engine

---

## INTRODUCTION

**Project developed in Python**

Template for a WebSocket server designed to stream real-time market data from **Interactive Brokers (IBAPI)** for analytical or dashboard-oriented applications.

This is a **demo version** and certain files or logic were intentionally excluded due to confidentiality agreements.

The original project was developed to **Sealiah Technologies**, a private company of Algorithmic Trading.

---

## RESUME

A flexible and scalable WebSocket-based architecture capable of ingesting, processing, and serving market data in real time.  
Both backend and frontend layers are intentionally modular, allowing the server to adapt to various market data providers, dashboard frameworks, and real-time analytical use cases

This enables the solution to scale according to customer requirements, technical constraints, or the nature of the market data provider.

Suitable for different business or project requirements.

---

## FUNCIONALITY

- Process SMA every new candle
- Log System for wether trades or any type of market information
- Multi Instruments (FX, Stocks, CMMDTY, etc.)
- Multi Frame

---

## Technology Stack (Replaceable)

### Backend

- IBAPI
- AsyncIO
- WebSockets
- Pandas / NumPy

### Frontend / Dashboards

- Bokeh

### Optional Integrations (ilustrative)

- Polygon.io
- Alpaca Markets
- Crypto WebSocket feeds
- Docker deployment
- Cloud Server
- Dash
- Streamlit
- SQL

> **NOTE:** Both backend and frontend can be replaced or extended depending on customer needs. Also programming languages.

---

## Possible Uses & Custom Modifications

- Tick Storage & Historical Databases
- Algorithmic Trading Strategies
- Market Analytics & Indicators
- News & Sentiment Feeds
- Portfolio Monitoring & Risk Tools
- Custom Dashboards or Client Applications

---
