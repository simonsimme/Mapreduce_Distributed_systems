# Basic - Group 13
This is our basic implementation of Lab 2: MapReduce.

## How to run 
1. **Navigate to the project directory**
```bash
cd ~/6.5840 
cd src/main
```
2. **Build the plugin and remove old outputs**
```bash
go build -buildmode=plugin ../mrapps/wc.go
rm mr-out*
```
3. **Run cordinator**
```bash 
go run -race mrcoordinator.go pg-*.txt
```

4. **Run the worker**
```bash 
go run mrworker.go wc.so
```

## How to run the tests 
Run the tests with:
```bash
cd 2.5840/src/main
bash test-mr.sh
```