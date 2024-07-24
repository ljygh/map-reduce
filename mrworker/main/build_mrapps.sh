go build -buildmode=plugin -o ./apps/wc.so ../mrapps/wc.go
go build -buildmode=plugin -o ./apps/rtiming.so ../mrapps/rtiming.go 
go build -buildmode=plugin -o ./apps/nocrash.so ../mrapps/nocrash.go 
go build -buildmode=plugin -o ./apps/mtiming.so ../mrapps/mtiming.go
go build -buildmode=plugin -o ./apps/jobcount.so ../mrapps/jobcount.go 
go build -buildmode=plugin -o ./apps/indexer.so ../mrapps/indexer.go 
go build -buildmode=plugin -o ./apps/early_exit.so ../mrapps/early_exit.go 
go build -buildmode=plugin -o ./apps/crash.so ../mrapps/crash.go