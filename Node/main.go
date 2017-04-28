package main

//import library golang
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

//status node
type NodeState int

const (
	LEADER    NodeState = iota
	CANDIDATE
	FOLLOWER
)

//variable global untuk tiap node
var currentNodeState = FOLLOWER
var currentTerm = 0
var votedFor = ""
var nodeAddresses = []string{}

//konstanta node
const (
	WORKER_TIME_LIMIT       = 1 * time.Hour
	ELECTION_TIME_LIMIT_MIN = 100 * time.Millisecond
	ELECTION_TIME_LIMIT_MAX = 3 * time.Second
	REQUEST_VOTE_INTERVAL   = 50 * time.Millisecond
	HEARTBEAT_INTERVAL      = 10 * time.Millisecond
	WORKER_PORT             = ":5555"
	NODE_PORT               = ":5556"
	CLIENT_PORT             = ":5557"
	UDP_BUFFER_SIZE         = 1024
	THREAD_POOL_NUM         = 3
	CURRENT_ADDRESS         = "192.168.1.14:5556"
	LOG_FILENAME            = "logs.txt"
	NODES_FILENAME = "nodes.txt"
)

//status message yang akan dikirim
type NodeMessageType int

const (
	VOTE_REQUEST            NodeMessageType = iota
	VOTE_RESPONSE
	APPEND_ENTRIES_REQUEST
	APPEND_ENTRIES_RESPONSE
	UNKNOWN
)

var registerVote = make(chan string)
var cancelElection = make(chan bool)
var getRPC = make(chan bool)
var grantVote = make(chan string)
var overthrowLeader = make(chan bool)

//untuk JSON
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
	}

	return json.Marshal(s)
}

//command untuk log
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

//log entry
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

//clean code sehingga code sudah cukup jelas
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

//implementasi interface heap untuk mengambil cpu usage worker paling kecil
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

//untuk load balancer
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
	workerLogs = append(workerLogs, Log{
		Worker:  worker,
		Term:    currentTerm,
		Command: commandType,
		Id:      lastApplied})
	lastApplied++
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
		CommitLog(entry)
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func CommitLog(log Log) {
	switch log.Command {
	case ADD:
		loadBalancer.Add(log.Worker)
	case DEL:
		loadBalancer.Delete(log.Worker.Address)
	case UPD:
		loadBalancer.Update(log.Worker.Address, log.Worker.CpuLoad)
	}
	if log.Id > commitIndex {
		commitIndex = log.Id
	}

	AppendToFile(log)
}

func HandleWorkerConn(buf []byte, n int) {
	log.Println(string(buf))
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
		AddLog(ADD, Worker{Address: workerAddress, CpuLoad: msg.CpuLoad})
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

//menangani koneksi dari node lain
func HandleNodeConn(buf []byte, n int) {
	log.Println(string(buf))
	var msg NodeMessage
	if err := json.Unmarshal(buf[:n], &msg); err != nil {
		fmt.Println(err)
		return
	}
	if currentNodeState == CANDIDATE && (msg.Term > currentTerm || (msg.Term == currentTerm && msg.IsFromLeader)) {
		currentNodeState = FOLLOWER
		cancelElection <- true
	}
	if currentNodeState == LEADER && msg.Term > currentTerm {
		currentNodeState = FOLLOWER
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
			prevLogTerm := workerLogs[nextIndex[i]-1].Term
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

		for i := commitIndex; i <= msg.CommitIndex; i++ {
			CommitLog(workerLogs[i])
		}
		commitIndex = msg.CommitIndex

		sendNodeMessage(
			NodeMessage{
				Success:      false,
				PrevLogIndex: lastApplied,
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
	http.Redirect(w, r, "http://"+workerAddress, http.StatusFound)
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

func requestVotes(hasVoted map[string]bool) {
	for _, nodeAddress := range nodeAddresses {
		if _, voted := hasVoted[nodeAddress]; !voted && nodeAddress != CURRENT_ADDRESS {
			requestVote(nodeAddress)
		}
	}
}

func startNewElectionTerm(electionResult chan bool) {
	currentTerm++
	hasVoted := make(map[string]bool)
	hasVoted[CURRENT_ADDRESS] = true
	go requestVotes(hasVoted)
	onCollectingVotes := true
	for onCollectingVotes {
		select {
		case <-cancelElection:
			onCollectingVotes = false
			electionResult <- false
		case voterAddress := <-registerVote:
			hasVoted[voterAddress] = true
		case <-time.After(randomDuration(ELECTION_TIME_LIMIT_MIN, ELECTION_TIME_LIMIT_MAX)):
			defer startNewElectionTerm(electionResult)
			onCollectingVotes = false
		}
		if len(hasVoted) >= len(nodeAddresses)/2+1 {
			currentNodeState = LEADER
			onCollectingVotes = false
			electionResult <- true
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
		var prevLogTerm int
		if len(workerLogs) == 0 {
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

func startRaft() {
	electionResult := make(chan bool)
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
			case candidateIPAddress := <-grantVote:
				sendVote(candidateIPAddress)
			case <-time.After(randomDuration(ELECTION_TIME_LIMIT_MIN, ELECTION_TIME_LIMIT_MAX)):
				currentNodeState = CANDIDATE
				startNewElectionTerm(electionResult)
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
		nodeAddresses = append(nodeAddresses, scanner.Text())
		nextIndex = append(nextIndex, 0)
		matchIndex = append(matchIndex, -1)
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

//main program
func main() {
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
