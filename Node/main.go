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
)

type NodeState int

const (
	LEADER    NodeState = iota
	CANDIDATE
	FOLLOWER
)

var currentNodeState = FOLLOWER
var currentTerm = 0
var votedFor = ""
var nodeAddresses = []string{"192.168.0.0", "192.168.0.0"}

const (
	WORKER_TIME_LIMIT       = 1 * time.Hour
	ELECTION_TIME_LIMIT_MIN = 150 * time.Millisecond
	ELECTION_TIME_LIMIT_MAX = 300 * time.Millisecond
	REQUEST_VOTE_INTERVAL   = 50 * time.Millisecond
	WORKER_PORT             = ":5555"
	NODE_PORT               = ":5556"
	CLIENT_PORT             = ":5557"
	UDP_BUFFER_SIZE         = 1024
	THREAD_POOL_NUM         = 3
	CURRENT_ADDRESS         = "192.168.0.0"
)

type NodeMessageType int

const (
	VOTE_REQUEST  NodeMessageType = iota
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

type Log struct {
	Id      int
	Command CommandType
	Term    int
	Worker  Worker
}

var workerLogs = []Log{}

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
	commitIndex     bool
	Entries         []Log
}

type Worker struct {
	cpuLoad float64
	address string
}

type WorkerHeapNode struct {
	worker         Worker
	addressMap     map[string]int
	lastUpdateTime time.Time
}

type WorkerHeap []WorkerHeapNode

func (h WorkerHeap) Len() int           { return len(h) }
func (h WorkerHeap) Less(i, j int) bool { return h[i].worker.cpuLoad < h[j].worker.cpuLoad }
func (h WorkerHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].addressMap[h[i].worker.address] = i
	h[j].addressMap[h[j].worker.address] = j
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

type LoadBalancer struct {
	sync.RWMutex
	workerHeap WorkerHeap
	addressMap map[string]int
}

func (l *LoadBalancer) Add(worker Worker) {
	l.Lock()
	l.addressMap[worker.address] = l.workerHeap.Len()
	heap.Push(&l.workerHeap, WorkerHeapNode{worker, l.addressMap, time.Now()})
	l.Unlock()
}

func (l *LoadBalancer) Update(workerAddress string, workerCPULoad float64) {
	l.Lock()
	i := l.addressMap[workerAddress]
	l.workerHeap[i].worker.cpuLoad = workerCPULoad
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
	for time.Now().Sub(l.workerHeap.Top().lastUpdateTime) > WORKER_TIME_LIMIT {
		l.RUnlock()
		l.Delete(l.workerHeap.Top().worker.address)
		l.RLock()
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

	workerLogs = append(workerLogs, Log{})
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
		loadBalancer.Add(worker)
	} else {
		loadBalancer.Update(workerAddress, msg.CpuLoad)
	}
}

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
	case APPEND_ENTRIES_REQUEST:
	}
}

func HandleClientConn(w http.ResponseWriter, r *http.Request) {
	if loadBalancer.IsEmpty() {
		fmt.Fprintf(w, "Ups, no Worker :(")
		return
	}
	workerAddress := loadBalancer.GetMinLoad().address
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

func requestVotes(endRequestingVote chan bool, hasVoted map[string]bool) {
	onRequestingVote := true
	for onRequestingVote {
		for _, nodeAddress := range nodeAddresses {
			if _, voted := hasVoted[nodeAddress]; !voted && nodeAddress != CURRENT_ADDRESS {
				requestVote(nodeAddress)
			}
		}
		select {
		case <-endRequestingVote:
			onRequestingVote = false
		case <-time.After(REQUEST_VOTE_INTERVAL):
		}
	}
}

func startNewElectionTerm(electionResult chan bool) {
	currentTerm++
	endRequestingVote := make(chan bool)
	hasVoted := make(map[string]bool)
	hasVoted[CURRENT_ADDRESS] = true
	go requestVotes(endRequestingVote, hasVoted)
	onCollectingVotes := true
	for onCollectingVotes {
		select {
		case <-cancelElection:
			onCollectingVotes = false
			endRequestingVote <- true
			electionResult <- false
		case voterAddress := <-registerVote:
			hasVoted[voterAddress] = true
		case <-time.After(randomDuration(ELECTION_TIME_LIMIT_MIN, ELECTION_TIME_LIMIT_MAX)):
			defer startNewElectionTerm(electionResult)
			onCollectingVotes = false
			endRequestingVote <- true
		}
		if len(hasVoted) >= len(nodeAddresses)/2+1 {
			currentNodeState = LEADER
			onCollectingVotes = false
			endRequestingVote <- true
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

func startRaft() {
	electionResult := make(chan bool)
	for {
		switch currentNodeState {
		case LEADER:
			clearBoolChannel(overthrowLeader)
			select {
			case <-overthrowLeader:
				currentNodeState = FOLLOWER
			}
		case CANDIDATE:
			clearBoolChannel(electionResult)
			select {
			case result := <-electionResult:
				if result == true {
					currentNodeState = LEADER
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

func main() {
	rand.Seed(time.Now().Unix())
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
