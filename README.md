[![ci](https://github.com/fgrzl/kv/actions/workflows/ci.yml/badge.svg)](https://github.com/fgrzl/kv/actions/workflows/ci.yml)
[![Dependabot Updates](https://github.com/fgrzl/kv/actions/workflows/dependabot/dependabot-updates/badge.svg)](https://github.com/fgrzl/kv/actions/workflows/dependabot/dependabot-updates)

# KV

This library provides a simple and flexible **key-value store abstraction** with support for CRUD operations, batch writes, range queries, and efficient enumeration.

The KV interface allows you to interact with different backends (e.g., Pebble, etc) seamlessly.

---

## 🚀 **Features**

- 🔑 Basic CRUD operations (`Get`, `Put`, `Remove`)
- ⚡ Batch operations with deduplication support
- 🔍 Range and prefix queries
- 🔄 Efficient item enumeration
- 🛠️ Support for custom query operators (e.g., `GreaterThan`, `Between`, `StartsWith`)

---

## 📦 **Installation**

```bash
go get github.com/fgrzl/kv
