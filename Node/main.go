package main

import (
	"fmt"
	"container/heap"
	"time"
	"net"
	"log"
	"net/http"
	"encoding/json"
	"strings"
	"sync"
	"math/rand"
	"path/filepath"
	"os"
	"bufio"
)

// Node status.
type NodeState int

const (
	LEADER    NodeState = iota
	CANDIDATE
	FOLLOWER
)

// Global variable for all nodes.
var currentNodeState = FOLLOWER
var currentTerm = 0
var votedFor = ""
var nodeAddresses = []string{}

// Node constants.
const (
	WORKER_TIME_LIMIT       = 10 * time.Second
	ELECTION_TIME_LIMIT_MIN = 150 * time.Millisecond
	ELECTION_TIME_LIMIT_MAX = 300 * time.Millisecond
	REQUEST_VOTE_INTERVAL   = 50 * time.Millisecond
	HEARTBEAT_INTERVAL      = 5 * time.Millisecond
	WORKER_PORT             = ":5555"
	NODE_PORT               = ":5556"
	CLIENT_PORT             = ":5557"
	UDP_BUFFER_SIZE         = 1024 * 1024
	THREAD_POOL_NUM         = 3
	CURRENT_ADDRESS         = "192.168.1.10:5556"
	LOG_FILENAME            = "logs.txt"
	NODES_FILENAME          = "nodes.txt"
)

// Message status that will be sent.
type NodeMessageType int

const (
	VOTE_REQUEST            NodeMessageType = iota
	VOTE_RESPONSE
	APPEND_ENTRIES_REQUEST
	APPEND_ENTRIES_RESPONSE
	UNKNOWN
)

var registerVote = make(chan string, 5)
var cancelElection = make(chan bool, 5)
var getRPC = make(chan bool, 5)
var grantVote = make(chan string, 5)
var overthrowLeader = make(chan bool, 5)

// JSON processing.
func (n *NodeMessageType) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	switch strings.ToLower(s) {
	default:
		*n = UNKNOWN
	case "vote_request":
		*n = VOTE_REQUEST
	case "vote_response":
		*n = VOTE_RESPONSE
	case "append_entries_request":
		*n = APPEND_ENTRIES_REQUEST
	case "append_entries_response":
		*n = APPEND_ENTRIES_RESPONSE
	}

	return nil
}

func (n NodeMessageType) MarshalJSON() ([]byte, error) {
	var s string
	switch n {
	default:
		s = "unknown"
	case VOTE_REQUEST:
		s = "vote_request"
	case VOTE_RESPONSE:
		s = "vote_response"
	case APPEND_ENTRIES_REQUEST:
		s = "append_entries_request"
	case APPEND_ENTRIES_RESPONSE:
		s = "append_entries_response"
	}

	return json.Marshal(s)
}

// Command for logging.
type CommandType int

const (
	ADD CommandType = iota
	DEL
	UPD
	XXX
)

func (n *CommandType) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	switch strings.ToLower(s) {
	default:
		*n = XXX
	case "add":
		*n = ADD
	case "del":
		*n = DEL
	case "upd":
		*n = UPD
	}

	return nil
}

func (n CommandType) MarshalJSON() ([]byte, error) {
	var s string
	switch n {
	default:
		s = "xxx"
	case ADD:
		s = "add"
	case DEL:
		s = "del"
	case UPD:
		s = "upd"
	}

	return json.Marshal(s)
}

// Log entry.
type Log struct {
	Id      int
	Command CommandType
	Term    int
	Worker  Worker
}

var workerLogs = []Log{}
var nextIndex = []int{}
var matchIndex = []int{}
var lastApplied = -1
var commitIndex = -1

// WorkerMessage as worker message.
type WorkerMessage struct {
	Port            int `json:"PORTDAEMON"`
	OriginIPAddress string `json:"IPDAEMON"`
	WorkerType      string `json:"TYPE"`
	CpuLoad         float64 `json:"CPUUSAGE"`
}

type NodeMessage struct {
	Type            NodeMessageType
	OriginIPAddress string
	Term            int
	IsFromLeader    bool
	LastLogIndex    int
	LastLogTerm     int
	CommitIndex     int
	Entries         []Log
	PrevLogIndex    int
	PrevLogTerm     int
	Success         bool
}

type Worker struct {
	CpuLoad float64
	Address string
}

// Heap interface implementation to extract CPU usage of the smallest worker.
type WorkerHeapNode struct {
	worker         Worker
	addressMap     map[string]int
	lastUpdateTime time.Time
}

type WorkerHeap []WorkerHeapNode

func (h WorkerHeap) Len() int           { return len(h) }
func (h WorkerHeap) Less(i, j int) bool { return h[i].worker.CpuLoad < h[j].worker.CpuLoad }
func (h WorkerHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].addressMap[h[i].worker.Address] = i
	h[j].addressMap[h[j].worker.Address] = j
}

func (h *WorkerHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(WorkerHeapNode))
}

func (h *WorkerHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0: n-1]
	return x
}

func (h WorkerHeap) Top() WorkerHeapNode {
	return h[0]
}

// LoadBalancer.
type LoadBalancer struct {
	sync.RWMutex
	workerHeap WorkerHeap
	addressMap map[string]int
}

func (l *LoadBalancer) Add(worker Worker) {
	l.Lock()
	l.addressMap[worker.Address] = l.workerHeap.Len()
	heap.Push(&l.workerHeap, WorkerHeapNode{worker, l.addressMap, time.Now()})
	l.Unlock()
}

func (l *LoadBalancer) Update(workerAddress string, workerCPULoad float64) {
	l.Lock()
	i := l.addressMap[workerAddress]
	l.workerHeap[i].worker.CpuLoad = workerCPULoad
	l.workerHeap[i].lastUpdateTime = time.Now()
	heap.Fix(&l.workerHeap, i)
	l.Unlock()
}

func (l *LoadBalancer) Delete(workerAddress string) {
	l.Lock()
	i := l.addressMap[workerAddress]
	delete(l.addressMap, workerAddress)
	heap.Remove(&l.workerHeap, i)
	l.Unlock()
}

func (l *LoadBalancer) Exist(workerAddress string) bool {
	l.RLock()
	if _, ok := l.addressMap[workerAddress]; ok {
		l.RUnlock()
		return true
	}
	l.RUnlock()
	return false
}

func (l *LoadBalancer) IsEmpty() bool {
	l.RLock()
	if l.workerHeap.Len() == 0 {
		l.RUnlock()
		return true
	}
	l.RUnlock()
	return false
}

func (l LoadBalancer) GetMinLoad() Worker {
	l.RLock()
	if time.Now().Sub(l.workerHeap.Top().lastUpdateTime) > WORKER_TIME_LIMIT {
		AddLog(DEL, l.workerHeap.Top().worker)
	}
	worker := l.workerHeap.Top().worker
	l.RUnlock()
	return worker
}

func CreateLoadBalancer() LoadBalancer {
	h := WorkerHeap{}
	m := make(map[string]int)
	heap.Init(&h)
	ret := LoadBalancer{workerHeap: h, addressMap: m}
	return ret
}

var loadBalancer LoadBalancer = CreateLoadBalancer()

func AddLog(commandType CommandType, worker Worker) {
	lastApplied++
	workerLogs = append(workerLogs, Log{
		Worker:  worker,
		Term:    currentTerm,
		Command: commandType,
		Id:      lastApplied})
}

func AppendToFile(log Log) {
	logJson, _ := json.Marshal(log)
	logJson = append(logJson, byte('\n'))
	if _, err := logFile.Write(logJson); err != nil {
		panic(err)
	}
}

func ReadAllLogFromFile() {
	absPath, _ := filepath.Abs(LOG_FILENAME)
	fmt.Println(absPath)
	file, err := os.OpenFile(absPath, os.O_APPEND|os.O_RDONLY, 0600)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry Log
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			fmt.Println(err)
			return
		}
		workerLogs = append(workerLogs, entry)
		CommitLogWithoutAppend(entry)
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func CommitLogWithoutAppend(log Log) {
	switch log.Command {
	case ADD:
		if !loadBalancer.Exist(log.Worker.Address) {
			loadBalancer.Add(log.Worker)
		} else {
			loadBalancer.Update(log.Worker.Address, log.Worker.CpuLoad)
		}
	case DEL:
		loadBalancer.Delete(log.Worker.Address)
	case UPD:
		loadBalancer.Update(log.Worker.Address, log.Worker.CpuLoad)
	}
	if log.Id > commitIndex {
		commitIndex = log.Id
	}
}

func CommitLog(log Log) {
	CommitLogWithoutAppend(log)
	AppendToFile(log)
}

func HandleWorkerConn(buf []byte, n int) {
	var msg WorkerMessage
	if err := json.Unmarshal(buf[:n], &msg); err != nil {
		fmt.Println(err)
		return
	}
	workerAddress := fmt.Sprintf("%s:%d", msg.OriginIPAddress, msg.Port)
	if currentNodeState != LEADER {
		return
	}
	if !loadBalancer.Exist(workerAddress) {
		worker := Worker{
			msg.CpuLoad,
			workerAddress}
		AddLog(ADD, worker)
	} else {
		AddLog(UPD, Worker{Address: workerAddress, CpuLoad: msg.CpuLoad})
	}
}

func getMajorityMatchIndex() int {
	freqMap := map[int]int{}
	for _, mi := range matchIndex {
		freqMap[mi]++
	}
	for index, freq := range freqMap {
		if freq > len(matchIndex)/2 {
			return index
		}
	}
	return commitIndex
}

func commitLeader(fromIndex int) {
	for i := fromIndex + 1; i <= commitIndex; i++ {
		CommitLog(workerLogs[i])
	}
}

func checkCommitIndexMajority() {
	defer commitLeader(commitIndex)
	upper := getMajorityMatchIndex()
	for i := commitIndex + 1; i < upper; i++ {
		if workerLogs[i].Term != currentTerm {
			return
		}
		commitIndex = i
	}
}

// Handles connection from other nodes.
func HandleNodeConn(buf []byte, n int) {
	var msg NodeMessage
	if err := json.Unmarshal(buf[:n], &msg); err != nil {
		fmt.Println(err)
		return
	}
	if currentNodeState == CANDIDATE && (msg.Term > currentTerm || (msg.Term >= currentTerm && msg.IsFromLeader)) {
		log.Println("CANCEL ELECTION")
		currentNodeState = FOLLOWER
		clearBoolChannel(cancelElection)
		cancelElection <- true
	}
	if currentNodeState == LEADER && msg.Term > currentTerm {
		currentNodeState = FOLLOWER
		clearBoolChannel(overthrowLeader)
		overthrowLeader <- true
	}

	switch msg.Type {
	case VOTE_RESPONSE:
		if currentNodeState == CANDIDATE {
			registerVote <- msg.OriginIPAddress
		}
	case VOTE_REQUEST:
		if msg.Term > currentTerm {
			currentTerm = msg.Term
			votedFor = ""
		}
		if msg.Term == currentTerm && (votedFor == "" || votedFor == msg.OriginIPAddress) {
			votedFor = msg.OriginIPAddress
			grantVote <- msg.OriginIPAddress

		}
	case APPEND_ENTRIES_RESPONSE:
		if msg.Success {
			var i int = 0
			for j, address := range nodeAddresses {
				if address == msg.OriginIPAddress {
					i = j
				}
			}
			nextIndex[i] = msg.PrevLogIndex + 1
			matchIndex[i] = msg.PrevLogIndex
			checkCommitIndexMajority()
		} else {
			if currentTerm < msg.Term && currentNodeState == LEADER {
				currentNodeState = FOLLOWER
				overthrowLeader <- true
				return
			}
			var sendEntries []Log
			for j := msg.PrevLogIndex + 1; j <= lastApplied; j++ {
				sendEntries = append(sendEntries, workerLogs[j])
			}
			var i int = 0
			for j, address := range nodeAddresses {
				if address == msg.OriginIPAddress {
					i = j
				}
			}
			nextIndex[i] = msg.PrevLogIndex + 1
			prevLogTerm := currentTerm
			if len(workerLogs) > nextIndex[i]-1 && nextIndex[i] != 0 {
				prevLogTerm = workerLogs[nextIndex[i]-1].Term
			}
			sendNodeMessage(
				NodeMessage{
					Term:            currentTerm,
					Type:            APPEND_ENTRIES_REQUEST,
					IsFromLeader:    true,
					OriginIPAddress: CURRENT_ADDRESS,
					PrevLogIndex:    nextIndex[i] - 1,
					PrevLogTerm:     prevLogTerm,
					CommitIndex:     commitIndex,
					Entries:         sendEntries},
				msg.OriginIPAddress)
		}
	case APPEND_ENTRIES_REQUEST:
		clearBoolChannel(getRPC)
		getRPC <- true
		if msg.Term < currentTerm {
			sendResponse(false, msg.OriginIPAddress)
			return
		}
		if msg.PrevLogIndex != lastApplied {
			sendNodeMessage(
				NodeMessage{
					Term:            currentTerm,
					Type:            APPEND_ENTRIES_RESPONSE,
					OriginIPAddress: CURRENT_ADDRESS,
					PrevLogIndex:    lastApplied,
					Success:         false,
				},
				msg.OriginIPAddress)
			return
		}
		if lastApplied != -1 && workerLogs[lastApplied].Term != msg.PrevLogTerm {
			lastApplied--
			sendNodeMessage(
				NodeMessage{
					Term:            currentTerm,
					Type:            APPEND_ENTRIES_RESPONSE,
					OriginIPAddress: CURRENT_ADDRESS,
					PrevLogIndex:    lastApplied,
					Success:         false,
				},
				msg.OriginIPAddress)
			return
		}

		for _, entry := range msg.Entries {
			if lastApplied+1 == len(workerLogs) {
				workerLogs = append(workerLogs, entry)
			} else {
				workerLogs[lastApplied+1] = entry
			}
			lastApplied++
		}

		if msg.CommitIndex != -1 {
			for i := commitIndex + 1; i <= msg.CommitIndex; i++ {
				if i >= len(workerLogs) {
					commitIndex = i - 1
					sendNodeMessage(
						NodeMessage{
							Term:            currentTerm,
							Type:            APPEND_ENTRIES_RESPONSE,
							OriginIPAddress: CURRENT_ADDRESS,
							PrevLogIndex:    lastApplied,
							Success:         false,
						},
						msg.OriginIPAddress)
					return
				}
				CommitLog(workerLogs[i])
			}
			commitIndex = msg.CommitIndex
		}
		currentTerm = msg.Term

		sendNodeMessage(
			NodeMessage{
				Success:         true,
				Type:            APPEND_ENTRIES_RESPONSE,
				PrevLogIndex:    lastApplied,
				Term:            currentTerm,
				OriginIPAddress: CURRENT_ADDRESS,
			},
			msg.OriginIPAddress)

	}

}

func sendResponse(success bool, address string) {
	sendNodeMessage(NodeMessage{
		Success:      success,
		PrevLogIndex: lastApplied,
		Term:         currentTerm,
	},
		address)
}

func HandleClientConn(w http.ResponseWriter, r *http.Request) {
	if loadBalancer.IsEmpty() {
		fmt.Fprintf(w, "Ups, no Worker :(")
		return
	}
	workerAddress := loadBalancer.GetMinLoad().Address
	log.Println("Client Request, Redirected to " + workerAddress)

	http.Redirect(w, r, "http://"+workerAddress+r.URL.Path, http.StatusFound)
}

func ListenToWorker(ch chan bool) {
	for {
		pc, err := net.ListenPacket("udp", WORKER_PORT)
		if err != nil {
			fmt.Println(err)
		}
		buffer := make([]byte, UDP_BUFFER_SIZE)
		n, _, err := pc.ReadFrom(buffer)
		jobs <- Job{HandleWorkerConn, buffer, n}
		pc.Close()
	}
	ch <- true
}

func ListenToNode(ch chan bool) {
	for {
		pc, err := net.ListenPacket("udp", NODE_PORT)
		if err != nil {
			fmt.Println(err)
		}
		buffer := make([]byte, UDP_BUFFER_SIZE)
		n, _, err := pc.ReadFrom(buffer)
		jobs <- Job{HandleNodeConn, buffer, n}
		pc.Close()
	}
	ch <- true
}

func CreateClientServer() {
	http.HandleFunc("/", HandleClientConn)
	http.ListenAndServe(CLIENT_PORT, nil)
}

type Job struct {
	f   func([]byte, int)
	buf []byte
	n   int
}

var jobs = make(chan Job, 100)

func threadPoolWorker(jobs chan Job) {
	for j := range jobs {
		j.f(j.buf, j.n)
	}
}

func randomDuration(min, max time.Duration) time.Duration {
	duration := rand.Int63n(max.Nanoseconds()-min.Nanoseconds()) + min.Nanoseconds()
	return time.Duration(duration) * time.Nanosecond
}

func sendNodeMessage(message NodeMessage, targetAddress string) {
	if buffer, err := json.Marshal(message); err != nil {
		fmt.Println(err)
	} else {
		conn, err := net.Dial("udp", targetAddress)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer conn.Close()
		conn.Write(buffer)
	}
}

func requestVote(nodeAddress string) {
	sendNodeMessage(
		NodeMessage{
			Term:            currentTerm,
			Type:            VOTE_REQUEST,
			IsFromLeader:    false,
			OriginIPAddress: CURRENT_ADDRESS},
		nodeAddress)
}

func sendVote(nodeAddress string) {
	sendNodeMessage(
		NodeMessage{
			Term:            currentTerm,
			Type:            VOTE_RESPONSE,
			IsFromLeader:    false,
			OriginIPAddress: CURRENT_ADDRESS},
		nodeAddress)
}

func startNewElectionTerm(electionResult chan bool) {
	currentTerm++
	for _, nodeAddress := range nodeAddresses {
		if nodeAddress != CURRENT_ADDRESS {
			requestVote(nodeAddress)
		}
	}
	voteCount := 1
	for {
		select {
		case <-cancelElection:
			clearBoolChannel(electionResult)
			electionResult <- false
			return
		case <-registerVote:
			voteCount++
		case <-time.After(randomDuration(ELECTION_TIME_LIMIT_MIN, ELECTION_TIME_LIMIT_MAX)):
			startNewElectionTerm(electionResult)
			return
		}
		if voteCount >= len(nodeAddresses)/2+1 {
			clearBoolChannel(electionResult)
			electionResult <- true
			return
		}
	}
}

func clearBoolChannel(ch chan bool) {
	for len(ch) > 0 {
		<-ch
	}
}

func clearStringChannel(ch chan string) {
	for len(ch) > 0 {
		<-ch
	}
}

func sendHeartBeat() {
	for i, nodeAddress := range nodeAddresses {
		if nodeAddress != CURRENT_ADDRESS {
			var prevLogTerm int
			if len(workerLogs) == 0 || nextIndex[i] == 0 {
				prevLogTerm = currentTerm
			} else {
				prevLogTerm = workerLogs[nextIndex[i]-1].Term
			}
			var sendEntries []Log
			for j := nextIndex[i]; j < lastApplied; j++ {
				sendEntries = append(sendEntries, workerLogs[j])
			}
			sendNodeMessage(
				NodeMessage{
					Term:            currentTerm,
					Type:            APPEND_ENTRIES_REQUEST,
					IsFromLeader:    true,
					OriginIPAddress: CURRENT_ADDRESS,
					PrevLogIndex:    nextIndex[i] - 1,
					PrevLogTerm:     prevLogTerm,
					CommitIndex:     commitIndex,
					Entries:         sendEntries},
				nodeAddress)
		}
	}
}

func startRaft() {
	electionResult := make(chan bool)
	threshold := 0
	for {
		switch currentNodeState {
		case LEADER:
			sendHeartBeat()
			clearBoolChannel(overthrowLeader)
			select {
			case <-overthrowLeader:
				currentNodeState = FOLLOWER
			case <-time.After(HEARTBEAT_INTERVAL):
			}
		case CANDIDATE:
			clearBoolChannel(electionResult)
			select {
			case result := <-electionResult:
				if result == true {
					log.Println("Jackpot")
					currentNodeState = LEADER
					for i, _ := range nextIndex {
						nextIndex[i] = lastApplied + 1
						matchIndex[i] = 0
					}

				} else {
					currentNodeState = FOLLOWER
				}
			}
		case FOLLOWER:
			clearStringChannel(grantVote)
			select {
			case <-getRPC:
				threshold = 0
			case candidateIPAddress := <-grantVote:
				sendVote(candidateIPAddress)
			case <-time.After(randomDuration(ELECTION_TIME_LIMIT_MIN, ELECTION_TIME_LIMIT_MAX)):
				if threshold > 3 {
					currentNodeState = CANDIDATE
					go startNewElectionTerm(electionResult)
				} else {
					threshold++
				}
			}
		}
	}
}

var logFile *os.File

func ReadAllNodesFromFile() {
	absPath, _ := filepath.Abs(NODES_FILENAME)
	fmt.Println(absPath)
	file, err := os.OpenFile(absPath, os.O_APPEND|os.O_RDONLY, 0600)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		address := scanner.Text()
		if address != CURRENT_ADDRESS {
			nodeAddresses = append(nodeAddresses, address)
			nextIndex = append(nextIndex, 0)
			matchIndex = append(matchIndex, -1)
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func checkStatus() {
	for {
		switch currentNodeState {
		case FOLLOWER:
			log.Printf("State: Follower")
		case CANDIDATE:
			log.Printf("State: Candidate")
		case LEADER:
			log.Printf("State: Leader")
		}
		log.Printf("Current Term: %d", currentTerm)
		time.Sleep(1 * time.Second)
	}
}

// Main program.
func main() {
	go checkStatus()
	rand.Seed(time.Now().Unix())
	absPath, _ := filepath.Abs(LOG_FILENAME)
	fmt.Println(absPath)
	file, err := os.OpenFile(absPath, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	logFile = file
	defer logFile.Close()
	ReadAllLogFromFile()
	ReadAllNodesFromFile()
	for w := 0; w < THREAD_POOL_NUM; w++ {
		go threadPoolWorker(jobs)
	}
	go startRaft()
	go CreateClientServer()
	workerChannel := make(chan bool)
	go ListenToWorker(workerChannel)
	nodeChannel := make(chan bool)
	go ListenToNode(nodeChannel)
	log.Println("Server Started")
	<-workerChannel
	<-nodeChannel
}
