
rdbms/
├── cmd/                  
│   └── main.go           
├── storage/              
│   ├── file_manager.go   
│   ├── block.go          
│   ├── page.go           
│   └── storage_test.go   
├── query/                
│   ├── parser.go         
│   ├── executor.go       
│   └── query_test.go     
├── transaction/          
│   ├── transaction.go    
│   ├── log.go            
│   └── transaction_test.go
├── index/                
│   ├── btree.go          
│   ├── hash.go           
│   └── index_test.go     
├── cli/                  
│   ├── repl.go           
│   └── commands.go       
├── internal/             
│   ├── utils.go          
│   └── config.go         
├── go.mod                
└── README.md             
