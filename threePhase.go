package main

import (
	"fmt"
	"net"
	"net/rpc"
	"log"
	"time"
)

// Reply for getting response like OK messages from the RPC calls. 
type Reply struct{
	Data string
}

// Core Bully Algorithm structure. It contains functions registered for RPC.
// it also contains information regarding other sites
type ThreePhaseCommit struct{
	my_id int
	coordinator_id int
	ids_ip map[int]string
	reply map[int]*Reply
}

// if a site has already invoked election, it doesnt need to start elections again
var no_election_invoked = true

var isInPrecommitState = false
var global_abort_commit_invoked = false

// GlobalCommit function invokes global commit instruction to the participant. Called by the coordinator
func (three_phase *ThreePhaseCommit) GlobalCommit(coordinator_id int, reply *Reply) error{
	global_abort_commit_invoked = true
	fmt.Println("Global commit issued")
	return nil
}

// GlobalAbort function invokes global abort instruction to the participant. Called by the coordinator
func (three_phase *ThreePhaseCommit) GlobalAbort(coordinator_id int, reply *Reply) error{
	global_abort_commit_invoked = true
	fmt.Println("Global abort issued")
	return nil
}

// Prepare to commit prepares the participants, including the coordinator to prepare for the commit
// it also executes waiting check that will call the leader election after timeout.

func (three_phase *ThreePhaseCommit) PrepareToCommit(coordinator_id int, reply *Reply) error{
	isInPrecommitState = true
	fmt.Println("Preparing to commit")
	fmt.Println("In pre-commit stage!")
	go three_phase.waitCheck()
	return nil
}

// Vote Request function is called by the coordinator through RPC. This asks the participant if it wants to issue
// Vote-commit or Vote-abort
// it also executes waiting check that will call the leader election after timeout.

func (three_phase *ThreePhaseCommit) VoteRequest(coordinator_id int, reply *Reply) error{
	fmt.Println("Vote Request from", coordinator_id)
	fmt.Printf("Do you to vote-commit? (y/n): ")
	answer := "y"
	fmt.Scanf("%s", &answer)
	if answer == "y"{
		reply.Data = "Vote-commit"
	}else{
		reply.Data = "Vote-abort"
		global_abort_commit_invoked = true
	}
	go three_phase.waitReadyCheck()
	return nil
}

// This function waits until timeout and executes election
func (three_phase *ThreePhaseCommit) waitReadyCheck(){
	time.Sleep(11 * time.Second)
	if global_abort_commit_invoked{
		return
	}	
	if isInPrecommitState {
		return		
	}
	fmt.Println("Timeout at Ready")
	invokeElection()
}

// This function waits until timeout and executes election
func (three_phase *ThreePhaseCommit) waitCheck(){
	time.Sleep(3 * time.Second)
	if global_abort_commit_invoked{
		return
	}	
	fmt.Println("Timeout at Precommit")
	invokeElection()
}
var allVotedCommit = true 		// a check to ensure if all the participants have voted commit. Used by the coordinator 
var commitVotedIds = []int{}		// appends all the pariticipants who have issued vote-commit and are waiting for decision from coordinator

// This function is used by the coordinator to issue the vote request to all the participants 
// It also checks the responses from participants and makes decisions.
func (three_phase *ThreePhaseCommit) initializeRequest(){
	for id,ip := range three_phase.ids_ip{
		three_phase.reply[id] = &Reply{""}
		if id == three_phase.my_id{
			continue
		}
		fmt.Println("Log: Sending vote request to", id)
		client, error := rpc.Dial("tcp",ip)
		if error != nil{
			fmt.Println("Log:", id, "is not available for vote.")
			allVotedCommit = false
			continue
		}
		client.Go("ThreePhaseCommit.VoteRequest", three_phase.my_id, three_phase.reply[id], nil)
	}
	three_phase.wait()
	if !allVotedCommit{
		three_phase.abort()
	} else{
		three_phase.precommit()
//		time.Sleep(20 * time.Second)
		three_phase.commit()	
	}
	fmt.Println("Log: request voting phase complete")
	three_phase.reset()
}

// reset function is called after the voting phase is complete inorder to invoke the next voting request
func (three_phase *ThreePhaseCommit) reset(){
	allVotedCommit = true
	commitVotedIds = []int{}
	three_phase.reply = map[int]*Reply{}
	isInPrecommitState = false
}

// called for issuing precommit to all the participants async.
func (three_phase *ThreePhaseCommit) precommit(){
	fmt.Println("Heading to precommit")
	for id,ip := range three_phase.ids_ip{
		three_phase.reply[id] = &Reply{""}
		fmt.Println("Log: Sending prepare commit to id:", id)
		client, error := rpc.Dial("tcp", ip)
		if error != nil{
			fmt.Println("Log: Unable to connect to host:", id)
			continue
		}
		client.Go("ThreePhaseCommit.PrepareToCommit", three_phase.my_id, three_phase.reply[id], nil)
	}
}

// waits for responses from the participants on voting request.
// if the coordinator changes in the mean time, it doesnt take any decision after the timeout
// it checks the responses from the clients and issues appropriate response

func (three_phase *ThreePhaseCommit) wait(){
	time.Sleep(time.Second * 10)	
	if three_phase.coordinator_id != three_phase.my_id{
		fmt.Println("I am not longer the coordinator")
		allVotedCommit = false
		return
	}
	for id, reply := range three_phase.reply{
		if id == three_phase.my_id{
			reply.Data = "Vote-commit"
		}
		fmt.Println(id,":", reply.Data)
		if reply.Data == "Vote-abort"{
			allVotedCommit = false
		}else if reply.Data == ""{
			allVotedCommit = false
		}else{
			commitVotedIds = append(commitVotedIds, id)
		}
	}
}

// abort is called to issue global abort to all the participants because of any failure
func (three_phase *ThreePhaseCommit) abort(){
	reply := Reply{""}
	for _, id := range commitVotedIds{
		ip := three_phase.ids_ip[id]
		fmt.Println("Log: Sending global abort to id:", id)
		client, error := rpc.Dial("tcp", ip)
		if error != nil{
			fmt.Println("Log: Unable to connect to host:", id)
			continue
		}
		client.Go("ThreePhaseCommit.GlobalAbort", three_phase.my_id, &reply, nil)
	}
}

// commit is called by coordinator to issue global commit to all the participants
func (three_phase *ThreePhaseCommit) commit(){
	reply := Reply{""}
	for id,ip := range three_phase.ids_ip{
		fmt.Println("Log: Sending global commit to id:", id)
		client, error := rpc.Dial("tcp", ip)
		if error != nil{
			fmt.Println("Log: Unable to connect to host:", id)
			continue
		}
		client.Go("ThreePhaseCommit.GlobalCommit", three_phase.my_id, &reply, nil)
	}
}

// This is the election function which is invoked when a smaller host id requests for an election to this host
func (three_phase *ThreePhaseCommit) Election(invoker_id int, reply *Reply) error{
	fmt.Println("Log: Receiving election from", invoker_id)
	if invoker_id < three_phase.my_id{
		fmt.Println("Log: Sending OK to", invoker_id)
		reply.Data = "OK"				// sends OK message to the small site
		if no_election_invoked{
			no_election_invoked = false
			go invokeElection()			// invokes election to its higher hosts
		}
	}
	return nil
}

var superiorNodeAvailable = false				// Toggled when any superior host sends OK message

// This function invokes the election to its higher hosts. It sends its Id as the parameter while calling the RPC
func invokeElection(){
	for id,ip := range three_phase.ids_ip{
		reply := Reply{""}
		if id > three_phase.my_id{
			fmt.Println("Log: Sending election to", id)
			client, error := rpc.Dial("tcp",ip)
			if error != nil{
				fmt.Println("Log:", id, "is not available.")
				continue
			}
			err := client.Call("ThreePhaseCommit.Election", three_phase.my_id, &reply)
			if err != nil{
				fmt.Println(err)
				fmt.Println("Log: Error calling function", id, "election")
				continue
			}
			if reply.Data == "OK"{				// Means superior host exists
				fmt.Println("Log: Received OK from", id)
				superiorNodeAvailable = true
			}
		}
	}
	if !superiorNodeAvailable{					// if no superior site is active, then the host can make itself the coordinator
		makeYourselfCoordinator()
	}
	superiorNodeAvailable = false
	no_election_invoked = true					// reset the election invoked
}

// This function is called by the new Coordinator to update the coordinator information of the other hosts
func (three_phase *ThreePhaseCommit) NewCoordinator(new_id int, reply *Reply) error{
	three_phase.coordinator_id = new_id 
	fmt.Println("Log:", three_phase.coordinator_id, "is now the new coordinator")
	return nil
}

func (three_phase *ThreePhaseCommit) HandleCommunication(req_id int, reply *Reply) error{
	fmt.Println("Log: Receiving communication from", req_id)
	reply.Data = "OK"
	return nil
}

func communicateToCoordinator(){
	coord_id := three_phase.coordinator_id
	coord_ip := three_phase.ids_ip[coord_id]
	fmt.Println("Log: Communicating coordinator", coord_id)
	my_id := three_phase.my_id
	reply := Reply{""}
	client, err := rpc.Dial("tcp", coord_ip)
	if err != nil{
		fmt.Println("Log: Coordinator",coord_id, "communication failed!")
		fmt.Println("Log: Invoking elections")
		invokeElection()
		return
	}
	err = client.Call("ThreePhaseCommit.HandleCommunication", my_id, &reply)
	if err != nil || reply.Data != "OK"{
		fmt.Println("Log: Communicating coordinator", coord_id, "failed!")
		fmt.Println("Log: Invoking elections")
		invokeElection()
		return
	}
	fmt.Println("Log: Communication received from coordinator", coord_id)
}

// This function is called when the host decides that it is the coordinator.
// it broadcasts the message to all other hosts and updates the leader info, including its own host.
func makeYourselfCoordinator(){
	reply := Reply{""}
	for id, ip := range three_phase.ids_ip{
		client, error := rpc.Dial("tcp", ip)
		if error != nil{
			fmt.Println("Log:", id, "communication error")
			continue
		}
		client.Call("ThreePhaseCommit.NewCoordinator", three_phase.my_id, &reply)
		if isInPrecommitState{
			client.Go("ThreePhaseCommit.GlobalCommit", three_phase.my_id, &reply, nil)
		} else{
			client.Go("ThreePhaseCommit.GlobalAbort", three_phase.my_id, &reply, nil)
		}
	}
}

// Core object of three_phase algorithm initialized with all ip addresses of all other sites in the network
var n = 4
var three_phase = ThreePhaseCommit{
	my_id: 		1,
	coordinator_id: 3,
	ids_ip: 	map[int]string{	1:"127.0.0.1:3000", 2:"127.0.0.1:3001", 3:"127.0.0.1:3002", 4:"127.0.0.1:3003"}, 
	reply: 		map[int]*Reply{}}


func main(){
	my_id := 0
	fmt.Printf("Enter the site id[1-4]: ")			// initialize the host id at the run time
	fmt.Scanf("%d", &my_id)
	three_phase.my_id = my_id
	my_ip := three_phase.ids_ip[three_phase.my_id]
	address, err := net.ResolveTCPAddr("tcp", my_ip) 
	if err != nil{
		log.Fatal(err)
	}
	inbound, err := net.ListenTCP("tcp", address)
	if err != nil{
		log.Fatal(err)
	}
	rpc.Register(&three_phase)
	fmt.Println("server is running with IP address and port number:", address)
	go rpc.Accept(inbound) // Accepting connections from other hosts.
	
	if three_phase.my_id == 3{
		random := ""
		fmt.Printf("Press enter to initialize Vote Request: ")
		fmt.Scanf("%s", random)	
		three_phase.initializeRequest()
	}

	time.Sleep(600 * time.Second)
}
