#r "nuget: Akka.Fsharp"

open System
open Akka.Actor
open Akka.FSharp

type NodeMessage = 
    | Create
    | Join of int
    | Stabilize
    | StabilizeNotification of int
    | FindSuccessor of int
    | PredecessorUpdate of int
    | SuccessorUpdate of int
    | FixFingers
    | FixFingersNotification of int*int*int
    | FingerUpdate of int*int
    | SendRequest
    | Route of int*int
    | StartRequests

type SupervisorMsg = 
    | AddHop of int

let input1 = Environment.GetCommandLineArgs().[2]
let input2 = Environment.GetCommandLineArgs().[3]

let system = ActorSystem.Create("FSharp")
let numNodes = int input1
let m = 20
let maxHash = Convert.ToInt32(Math.Pow(2.0,float m))
let numRequests = int input2
let gap = maxHash/numNodes

printfn "%i %i %i" numNodes m maxHash

let r = Random()

let generateRequest() =
    r.Next(0,maxHash-1)

let Supervisor(mailbox: Actor<_>) =
    let rec loop hops req effHops effReq ()= actor{
        let! msg = mailbox.Receive();

        match msg with
        | AddHop x -> 
            if x <=m 
            then 
                if req%100 = 0 then printfn "TotalHops: %i, Total Requests: %i, Average Hops: %A, Efficient Hops: %i, Efficient Requests %i, Efficient Average Hops: %A" (hops+x) (req+1) (float (float(hops+x)/float(req+1))) (effHops+x) (effReq+1) (float (float(effHops+x)/float(effReq+1)))
                return! loop (hops+x) (req + 1) (effHops+x) (effReq+1) ()
            else 
                if req%100 = 0 then printfn "TotalHops: %i, Total Requests: %i, Average Hops: %A, Efficient Hops: %i, Efficient Requests %i, Efficient Average Hops: %A" (hops+x) (req+1) (float (float(hops+x)/float(req+1))) (effHops) (effReq) (float (float(effHops)/float(effReq)))
                return! loop (hops+x) (req + 1) (effHops) effReq ()

        return! loop hops req effHops effReq ()
    }
    loop 0 0 0 0 ()

let supervisorRef = spawn system "supervisor" Supervisor

let inBetween node a b = 
    let aMod = a%maxHash
    let bMod = b%maxHash
    let nodeMod = node%maxHash
    if a=b then true
    elif a<b then a<nodeMod && nodeMod<=b
    else not(b<nodeMod && nodeMod<=a)    

let getActorFromNode node = 
    let actorPath = @"akka://FSharp/user/" + "Node" + string (node%maxHash)
    select actorPath system

let closestPrecedingNode (fingerTable:int[]) newNode existingNode= 
    let result = Array.tryFind (fun elem -> inBetween elem existingNode newNode) (Array.rev fingerTable)
    match result with
    | Some x -> x
    | None -> -1

let create n = 
    (-1, n)

let join newNode existingNode = 
    let predecessor = -1
    let successor = newNode
    getActorFromNode existingNode <! FindSuccessor newNode
    (predecessor,successor)

let findSuccessor newNode existingNode (fingerTable:int[]) = 
    let successor = fingerTable.[0]
    if(inBetween newNode existingNode successor)
    then
        successor
    else
        let node = closestPrecedingNode fingerTable newNode existingNode
        if(node = -1)
        then
            existingNode
        else
            getActorFromNode node <! FindSuccessor newNode
            -1

let notify predecessor node notifier= 
    if predecessor = -1  || inBetween notifier predecessor node then getActorFromNode node <! PredecessorUpdate notifier

let stabilizeNotification node notifier predecessor =
    if inBetween predecessor notifier node && predecessor <> -1
    then
        getActorFromNode notifier <! SuccessorUpdate predecessor
    
    notify predecessor node notifier

let fixFinger limit notifier existingNode (fingerTable:int[]) finger = 
    // if(notifier = 5 && existingNode = 5 && finger = 5) then supervisorRef <! FingerTable(limit,finger,fingerTable,-1)
    let successor = fingerTable.[0]
    if(inBetween limit existingNode successor)
    then
        successor
    else
        let node = closestPrecedingNode fingerTable limit existingNode
        if(node = -1)
        then
            successor
        else
            getActorFromNode node <! FixFingersNotification (notifier,limit,finger)
            -1

let Node node (mailbox: Actor<_>) =
    let rec loop (fingerTable:int[]) fingerForUpdate predecessor count () = actor{
        let! msg = mailbox.Receive();
        let sender = mailbox.Sender()
        // system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(10000.0), mailbox.Self , FixFingers)
        match msg with
        | Create ->
            let (p,s) = create node
            fingerTable.[0] <- s
            return! loop fingerTable fingerForUpdate -1 count ()

        | Join x ->
            let (p,s) = join node x
            fingerTable.[0] <- s
            return! loop fingerTable fingerForUpdate -1 count ()

        | FindSuccessor x ->
            let s = findSuccessor x node fingerTable
            if(s <> -1)
            then
                getActorFromNode x <! SuccessorUpdate s

        | Stabilize ->
            // if node = 60 then supervisorRef <! Print(count, node, fingerTable.[0])
            // if node = 60 then supervisorRef <! Print(predecessor, node, fingerTable.[0])
            getActorFromNode fingerTable.[0] <! StabilizeNotification node
            return! loop fingerTable fingerForUpdate predecessor count ()

        | StabilizeNotification x ->
            stabilizeNotification node x predecessor
            return! loop fingerTable fingerForUpdate predecessor count ()

        | FixFingers ->
            getActorFromNode node <! FixFingersNotification (node,(node + int (Math.Pow(2.0,float fingerForUpdate))) % maxHash,fingerForUpdate)
            // system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(10000.0), mailbox.Self , FixFingers)
            if (fingerForUpdate>=m-1) then return! loop fingerTable 0 predecessor count ()
            else return! loop fingerTable (fingerForUpdate+1) predecessor count ()

        | FixFingersNotification (notifier, limit, finger)->
            let f = fixFinger limit notifier node fingerTable finger
            if(f <> -1) then getActorFromNode notifier <! FingerUpdate (finger,f)
            
        | FingerUpdate (finger,s)->
            // if(finger = 5) then supervisorRef <! FingerTable(node,s,fingerTable,-1)
            fingerTable.[finger] <- s
            return! loop fingerTable fingerForUpdate predecessor count ()

        | PredecessorUpdate p -> 
            return! loop fingerTable fingerForUpdate (p%maxHash) count ()

        | SuccessorUpdate s ->
            fingerTable.[0] <- s%maxHash
            return! loop fingerTable fingerForUpdate predecessor count ()

        | StartRequests ->
            system.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(1.0), TimeSpan.FromSeconds(1.0),mailbox.Self , SendRequest)

        | SendRequest ->
            if count<numRequests
            then
                let request = r.Next(1,maxHash-1)
                let nextHop = closestPrecedingNode fingerTable request node
                if(nextHop <> -1) then getActorFromNode nextHop <! Route (request,1)
                else supervisorRef <! AddHop 0
                return! loop fingerTable fingerForUpdate predecessor (count+1) ()

        | Route (request,hop) ->
            // if(hop>100) then printfn "%i" request
            let nextHop = closestPrecedingNode fingerTable request node

            if(nextHop <> -1) then getActorFromNode nextHop <! Route (request,hop+1)
            else supervisorRef <! AddHop hop

        return! loop fingerTable fingerForUpdate predecessor count ()
    }
    system.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(1.0), TimeSpan.FromSeconds(1.0),mailbox.Self , Stabilize)
    system.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(1.0), TimeSpan.FromSeconds(1.0),mailbox.Self , FixFingers)
    loop [|for i in 0 .. m-1 -> node|] 0 -1 0 ()

let nodes = [for i in 1..numNodes do gap*i]
for i in 0..numNodes-1 do
    let name = "Node" + string nodes.[i]
    spawn system name (Node nodes.[i]) |> ignore

// printf "%A" nodeActors

let starter = nodes.[numNodes-1]
getActorFromNode starter <! Create

for i = numNodes-2 downto 0 do
    let node = nodes.[i]
    getActorFromNode node <! Join nodes.[i+1]

System.Threading.Thread.Sleep(5000)

for i = numNodes-1 downto 0 do
    let node = nodes.[i]
    getActorFromNode node <! StartRequests

Console.ReadLine() |> ignore