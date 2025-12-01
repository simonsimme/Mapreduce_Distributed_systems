# Advanced - Group 13
This is our advanced implementation of Lab 2: MapReduce.

## How to run 
Our solution to this lab is intended to be set up on at least two nodes: one coordinator node and at least one worker node.
The IP of the coordinator node has to be known to each of the worker nodes. The coordinator will discover the worker nodes when the workers register with the coordinator during worker initialization. 
We were using AWS ES2 instances to repelicate the needed behavior, where each node is a seperate instance.
### Coordinator Node Setup 
1. **Navigate to the project directory**
```bash
cd ~/6.5840 
cd src/main
```
2. **Run cordinator**
```bash 
go run -race mrcoordinator.go pg-*.txt
```
3. **Save the IP**
```html
<p>Remember the public IP of this machine. It will be needed to update the worker's address.  
On AWS, you can easily find it in the EC2 dashboard.<p>
```


### Worker Node Setup
1. **Navigate to the project directory**
```bash
cd ~/6.5840 
```
2. **Set coordinator IP**
```bash
cd src/mr
nano worker.go 
```
Go to the worker.go and on the top of the file write the IP and port of to match the machine of your cordinator. Listed as variables cordinatorAddress and cordinatorPort
3. **Build the plugin and remove old outputs**
```bash
cd .. 
cd src/main
go build -buildmode=plugin ../mrapps/wc.go
rm mr-out*
```

4. **Finally, run the worker**
```bash 
go run mrworker.go wc.so
```





