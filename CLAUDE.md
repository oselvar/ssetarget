# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SSETarget is a TypeScript library for dispatching Server-Sent Events (SSE) to EventSource clients.
The library provides a simple API for real-time event streaming with automatic event persistence and replay for new clients.

The library also contains utilities for dispatching events from Cloudflare Workflow steps to monitor progress.

## Commands

### Development

- `pnpm start` - Start the demo server with Wrangler (port 9875)
- `pnpm test` - Run tests with Vitest
- `pnpm build` - Build the library with tsc
- `pnpm fix` - Run TypeScript check, format with oxfmt, and lint with oxlint
- `pnpm install` - Install dependencies (this project uses pnpm; see `packageManager` in `package.json`)

### Testing

- `vitest` - Run tests in watch mode
- `vitest run` - Run tests once
- `vitest --coverage` - Run tests with coverage report

### Type Generation

- `make src/examples/worker-configuration.d.ts` - Generate Cloudflare Worker types from wrangler.toml

## Architecture

### Core Components

**SSETarget (Abstract)** - The base class for SSE event dispatching:

- `dispatchEvent(event)` - Dispatches events to connected clients
- `fetch(request)` - Handles SSE HTTP requests using Hono
- Abstract methods: `storeEvent()` and `getEvents()` for persistence

**MemorySSETarget** - Simple in-memory implementation of SSETarget for basic use cases

**WorkflowEvents (Durable Object)** - Cloudflare-specific implementation using Durable Object storage:

- Persists events in SQLite via DurableObjectState
- Handles batched event dispatching
- Designed for production Cloudflare Workers

### Workflow Integration

**WorkflowEventStep** - Wraps Cloudflare WorkflowStep to automatically dispatch SSE events:

- Intercepts `do()`, `sleep()`, `sleepUntil()`, and `waitForEvent()` calls
- Sends "started", "completed", and "failed" events with step metadata
- Supports selective event dispatching via `ShouldDispatch` callback

**DemoWorkflow** - Example workflow showing SSE integration with sleep, wait, and do operations

### Event Flow

1. Workflows use `WorkflowEventStep` wrapper around standard `WorkflowStep`
2. Step operations automatically trigger SSE events via `batchedDispatchEvent`
3. Events are stored in WorkflowEvents Durable Object SQLite database
4. Clients connect to `/:instanceId/sse` endpoint to receive real-time events
5. New clients receive all historical events via `Last-Event-ID` header support

### Module Exports

The library has multiple export paths:

- Main: Core SSETarget and MemorySSETarget classes
- `./workflows`: Workflow-related types and utilities
- `./workflows/cloudflare`: Cloudflare-specific implementations
- `./workflows/cloudflare/sse`: SSE serving utilities

## Development Notes

- Uses strict TypeScript configuration with comprehensive type checking
- Built for ESNext modules with Cloudflare Workers compatibility
- Hono framework for HTTP routing and SSE streaming
- Vitest for testing with coverage reporting
- ESLint with import sorting and Prettier for code formatting
