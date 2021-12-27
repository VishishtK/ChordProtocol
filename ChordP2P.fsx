#r "nuget: Akka.Fsharp"

open System
open Akka.Actor
open Akka.FSharp

type NodeMessage = 
    | Create
    | Join of int
    | Stabilize
    | StabilizeNotification of int
    | FixFingers
    | FixFingersNotification of int * int * int
    | FindSuccessor of int
    | PredecessorUpdate of int
    | SuccessorUpdate of int
    | FingerUpdate of int * int

type SupervisorMsg = 
    | Print of int * int[] * int

let system = ActorSystem.Create("FSharp")
let numNodes = 16
let m =  Convert.ToInt32(Math.Ceiling(Math.Log(float numNodes, float 2)))
let maxHash = Convert.ToInt32(Math.Pow(2.0,float m))
// printfn "%i %i %i" numNodes m maxHash

let getActorFromNode node = 
    let actorPath = @"akka://FSharp/user/" + "Node" + string (node%maxHash)
    select actorPath system


let closestPrecedingNode (fingerTable:int[]) newNode = 
    let result = Array.tryFind (fun elem -> elem < newNode) (Array.rev fingerTable)
    match result with
    | Some x -> x
    | None -> newNode

let create n = 
    (-1, n)

let join newNode existingNode = 
    let predecessor = -1
    let successor = newNode
    getActorFromNode existingNode <! FindSuccessor newNode
    (predecessor,successor)

let stabilizeNotification node notifier predecessor =
    if notifier<predecessor && predecessor<node
    then 
        let actor = getActorFromNode  notifier
        actor <! SuccessorUpdate predecessor
        predecessor
    elif predecessor<notifier && notifier<node 
    then 
        notifier
    else 
        predecessor

// let fixFinger node lowerLimit =
//     let mutable i = node+lowerLimit
//     while i <> node do
//         if getActorFromNode i = ActorRefs.NoSender
//         then 
//             i <- (i+1) % maxHash

let fixFingersNotification (node:int) fingerTable notifier (limit:int)  finger= 
    if node >= limit
    then
        getActorFromNode notifier <! FingerUpdate(finger,node)
    else
        let find = Array.tryFind (fun elem -> elem > limit) fingerTable
        match find with
        | Some x ->
            getActorFromNode notifier <! FingerUpdate(finger,x)
        | None ->
            getActorFromNode (fingerTable.[m-1]) <! FixFingersNotification(notifier, limit, finger)

let findSuccessor newNode node successor fingerTable = 
    if(node<newNode && newNode<successor)
    then
        successor
    else
        let closestPrecedingNode = closestPrecedingNode fingerTable newNode
        if(closestPrecedingNode = newNode)
        then
            newNode
        else
            getActorFromNode closestPrecedingNode <! FindSuccessor newNode
            -1       

let Supervisor(mailbox: Actor<_>) =
    let rec loop ()= actor{
        let! msg = mailbox.Receive();
        let sender = mailbox.Sender()
        printfn "%A" msg
        return! loop ()
    }           
    loop ()

let supervisorRef = spawn system "supervisor" Supervisor

let Node node (mailbox: Actor<_>) =
    let rec loop (fingerTable:int[]) fingerNext predecessor successor () = actor{
        let! msg = mailbox.Receive();
        let sender = mailbox.Sender()
        
        match msg with
        | Create ->
            let (p,s) = create node
            return! loop fingerTable fingerNext (p%maxHash) (s%maxHash) ()

        | Join x ->
            let (p,s) = join node x
            return! loop fingerTable fingerNext (p%maxHash) s ()

        | Stabilize ->
            getActorFromNode successor <! StabilizeNotification
            system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(10.0), mailbox.Self , Stabilize)

        | StabilizeNotification x ->
            let p = stabilizeNotification node x predecessor
            return! loop fingerTable fingerNext (p%maxHash) successor ()

        | FixFingers ->
            let fingerLimit = node + int (Math.Pow(2.0,float fingerNext))
            getActorFromNode fingerTable.[fingerNext%m] <! FixFingersNotification(node, fingerLimit, fingerNext)
            system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(10.0), mailbox.Self , FixFingers)
            return! loop fingerTable ((fingerNext+1)%m) predecessor successor ()

        | FixFingersNotification (notifier, limit, finger)->
            fixFingersNotification node fingerTable notifier limit finger
            
        | FingerUpdate (finger,node)->
            fingerTable.[finger%m] <- node%maxHash
            if(finger%m=0)
            then supervisorRef <! Print(node,fingerTable,successor)
            return! loop fingerTable fingerNext predecessor successor ()

        | FindSuccessor x ->
            let s = findSuccessor x node successor fingerTable
            if(s <> -1)
            then
                getActorFromNode x <! SuccessorUpdate s
            
        | PredecessorUpdate p -> 
            return! loop fingerTable fingerNext (p%maxHash) successor ()

        | SuccessorUpdate s ->
            return! loop fingerTable fingerNext predecessor (s%maxHash) ()

        return! loop fingerTable fingerNext predecessor successor ()
    }
    // system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(10.0), mailbox.Self , Stabilize)
    system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(10.0), mailbox.Self , FixFingers)
    loop [| for i in 0 .. m-1 -> node+1 % numNodes |] 0 -1 node ()

let nodesActors = [
    for z in 0..numNodes-1 do
        let name = "Node" + string z
        spawn system name (Node z)
]

nodesActors.[(numNodes-1)%maxHash] <! Create

for node = numNodes-1 downto 0 do
    getActorFromNode node <! Join (node + 1)

Console.ReadLine() |> ignore