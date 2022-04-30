#time "on"
#r "nuget: Akka"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"

open System
open Akka.Actor
open Akka.FSharp

let numOfNodes = fsi.CommandLineArgs.[1] |> int
let numOfRequests = fsi.CommandLineArgs.[2] |> int
let systemImpl = ActorSystem.Create("chordImplementation")
let mutable numOfHops: Int64 = 0L
let mutable numOfLookups: Int64 = 0L
let mutable nodePool: IActorRef list = []

type DefTypes =
    | Initiate of (int)
    | Input
    | LookUpNext of (int * int)
    | EndLookUp
    | Search of (int)
   
let n: int =
    (Math.Log(numOfNodes |> float, 2.0)) |> int

printfn "Taking n as %d" n


let lookUpActor (mailbox: Actor<_>) =
    let rec loop () =
        actor {
            let! (message) = mailbox.Receive()

            match message with
            | EndLookUp ->
                numOfLookups <- numOfLookups + 1L

                let mutable totNum= (int64 numOfNodes) * (int64 numOfRequests)
                if numOfLookups = (totNum) then

                    printfn "number of requests completed : %d" numOfLookups
                    printfn "number of hops made : %d" numOfHops
                    printfn "Average number of hops : %f" (float (numOfHops) / float (numOfLookups))

                    mailbox.Context.System.Terminate() |> ignore
            | _ -> ()

            return! loop ()
        }

    loop ()

let SearchActorRef =
    spawn systemImpl "lookUpActor" lookUpActor


let ChildActor (mailbox: Actor<_>) =

    let mutable nID: int = 0
    let mutable fingerTable = []

    let rec loop () =
        actor {

            let! (message: DefTypes) = mailbox.Receive()
            let self = mailbox.Self
            let sender = mailbox.Sender()
            let a: DefTypes = message

            match a with
            | Search (k) ->
                
                let mutable i= 0
                let mutable bool = true

                if k = nID then
                    bool <- false
                    SearchActorRef <! EndLookUp
                
                else
                    while i < n do
                        if fingerTable.[i] = k then 
                            bool <- false
                            SearchActorRef <! EndLookUp
                        

                        else if i < n - 1 then

                            if (fingerTable.[i + 1] < fingerTable.[i] || (k< fingerTable.[i + 1] && k > fingerTable.[i])) then
                                
                                bool <- false
                                self <! LookUpNext(k, fingerTable.[i])

                        i <- i + 1

                       
                if bool then
                    self <! LookUpNext(k, fingerTable.[n - 1])

            | LookUpNext (k, node) ->
                numOfHops <- numOfHops + 1L
                nodePool.[node] <! Search(k)

            | Initiate (id) ->
                
                nID <- id
                let collection =
                    [ 0 .. n - 1 ]
                    |> List.map
                        (fun i ->
                            (float (nID) + (Math.Pow(2.0, float (i)))) % float (numOfNodes)
                            |> int)

                fingerTable <- List.sort collection

            | _ -> ()
            return! loop ()
        }

    loop ()

let FatherActor (mailbox: Actor<_>) =

    let rec loop () =
        actor {

            let! msg = mailbox.Receive()

            match msg with
            | Input ->

                printfn "node network in process"

                let register =
                    [ 1 .. numOfNodes ]
                    |> List.map (fun id -> spawn systemImpl (sprintf "Local_%d" id) ChildActor)

                for i = 0 to numOfNodes - 1 do
                    register.[i] <! Initiate(i)

                nodePool <- register

                let rand = Random()

                for j = 0 to numOfRequests - 1 do
                    for i = 0 to numOfNodes - 1 do
                        let randNumber = rand.Next(numOfNodes)
                        nodePool.[i] <! Search(randNumber)

                    // System.Threading.Thread.Sleep(1000)

            | _ -> ()

            return! loop ()
        }

    loop ()

let Main = spawn systemImpl "Main" FatherActor
Main <! Input

systemImpl.WhenTerminated.Wait()