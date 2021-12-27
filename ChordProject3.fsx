#r "nuget: Akka.Fsharp"

open System
open Akka.Actor
open Akka.FSharp

type Message = 
    | Create of int
    | Join of int
    | Stabilize
    | Notify of int
    | FixFingers
    | FindSuccessor of int
    | PredecessorUpdate of int
    | SuccessorUpdate of int
    | FingerUpdate of int * int
    // | CheckPredecessor
    
let system = ActorSystem.Create("FSharp")
let numNodes = 8
let m =  Convert.ToInt32(Math.Log(float numNodes, float 2))

let getActorFromNode node = 
    let actorPath = @"akka://FSharp/user/" + string node
    select actorPath system

let closestPrecedingNode (fingerTable:int[]) newNode = 
    let result = Array.tryFind (fun elem -> elem < newNode) (Array.rev fingerTable)
    match result with
    | Some x -> x
    | None -> newNode

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

let create n = 
    (-1, n)

let join newNode existingNode = 
    let predecessor = -1
    let successor = newNode
    getActorFromNode existingNode <! FindSuccessor newNode
    (predecessor,successor)

let notify node predecessor newPredecessor = 
    if(predecessor < newPredecessor && newPredecessor < node)
    then
        newPredecessor
    else
        predecessor

let stabilize node successor =
    let x = string (getActorFromNode successor <! Predecessor)
    if node < int x && int x < successor
    then 
        let actor = getActorFromNode  node
        actor <! SuccessorUpdate (int x)
    
    getActorFromNode successor <! notify node

let fixFinger finger fingerNext =
    findSuccessor finger fingerNext

// let checkPredecessor node = 
//     let nodePredecessor = string (getActorFromNode node <! Predecessor)
//     try
//         getActorFromNode nodePredecessor
//     with
//         | ex -> 
//             (getActorFromNode node) <! PredecessorUpdate -1
//             printfn "Actor dead"

let notify node predecessor notifier =
    if(predecessor = -1 || (predecessor<notifier && notifier<predecessor))
    then
        notifier
    else
        predecessor

let Node node (mailbox: Actor<_>) =
    let rec loop (fingerTable:int[]) fingerNext predecessor successor () = actor{
        let! msg = mailbox.Receive();
        let sender = mailbox.Sender()
        
        match msg with
        | Create x ->
            let (p,s) = create x
            return! loop fingerTable fingerNext p s ()
        | Join x ->
            let (p,s) = join node x
            return! loop fingerTable fingerNext p s ()
        | Stabilize ->
            stabilize node successor
        | Notify x ->
            let p = notify node predecessor x
            return! loop fingerTable fingerNext p successor ()
        | FixFingers ->
            fixFinger fingerTable.[fingerNext]
            if(fingerNext > m)
            then 
                return! loop fingerTable 1 predecessor successor ()
            else
                return! loop fingerTable fingerNext predecessor successor ()
        | FindSuccessor x ->
            let s = findSuccessor x node successor fingerTable
            if(s <> -1)
            then
                getActorFromNode x <! SuccessorUpdate s
            
        | PredecessorUpdate x -> 
            return! loop fingerTable fingerNext x successor ()
        | SuccessorUpdate x ->
            return! loop fingerTable fingerNext predecessor x ()
        | FingerUpdate (finger,node) ->
            fingerTable.[finger] <- node
            return! loop fingerTable fingerNext predecessor successor ()
        // | CheckPredecessor
        return! loop fingerTable fingerNext predecessor successor ()
    }   
    loop ()

