# RDBMS

A simple relational database management system implementation in Go.

## Project Structure

```
rdbms/
├── cmd/                  # Main application entry point
│   └── main.go          
├── storage/             # Storage engine implementation
│   ├── file_manager.go  # File management and I/O operations
│   ├── block.go         # Block level operations
│   ├── page.go          # Page management
│   └── storage_test.go  
├── query/               # Query processing and execution
│   ├── parser.go        # SQL query parser
│   ├── executor.go      # Query execution engine
│   └── query_test.go    
├── transaction/         # Transaction management
│   ├── transaction.go   # Transaction processing
│   ├── log.go          # Transaction logging
│   └── transaction_test.go
├── index/              # Index implementations
│   ├── btree.go        # B-tree index structure
│   ├── hash.go         # Hash index structure
│   └── index_test.go   
├── cli/               # Command-line interface
│   ├── repl.go        # REPL implementation
│   └── commands.go    # CLI commands
└── internal/          # Internal utilities
    ├── utils.go       # Helper functions
    └── config.go      # Configuration management
```

## Features

- File-based storage engine
- B-tree and Hash index support
- SQL query parsing and execution
- Transaction management with ACID properties
- Command-line interface with REPL

## Getting Started

To run the database:

```bash
go run cmd/main.go
```

## Testing

Run all tests with:

```bash
go test ./...
