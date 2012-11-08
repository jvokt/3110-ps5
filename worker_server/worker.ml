open Protocol
open Util

let activeWorkers = Hashtbl.create 16 

let mem = Hashtbl.mem

let find = Hashtbl.find

let m = Mutex.create ()

let send_response client response =
  let success = Connection.output client response in
    (if not success then
      (Connection.close client;
       print_endline "Connection lost before response could be sent.")
    else ());
    success

let mapper_builder source shared_data : worker_response = 
  match Program.build source with
  | (Some(id), _) -> 
      (Mutex.lock m;
      Hashtbl.add activeWorkers id "mapper"; 
      Program.write_shared_data id shared_data;
      Mutex.unlock m;
      Mapper(Some(id),""))
  | (None, error_msg) -> Mapper(None, error_msg) 

let reducer_builder source : worker_response =
  match Program.build source with 
  | (Some(id), _) -> 
      (Mutex.lock m;
      Hashtbl.add activeWorkers id "reducer"; 
      Mutex.unlock m;
      Reducer(Some(id),""))
  | (None, error_msg) -> Reducer(None, error_msg)

let map_requester client id results =
  match results with
  | None -> send_response client (RuntimeError(id,"Error"))
  | Some result -> send_response client (MapResults(id,result))
    
let red_requester client id results =
  match results with
  | None -> send_response client (RuntimeError(id,"Error"))
  | Some result -> send_response client (ReduceResults(id,result))

let rec handle_request client =
  match Connection.input client with
  | Some v ->
    (match v with
    | InitMapper (source, shared_data) -> 
        let built = mapper_builder source shared_data in
        if (send_response client built) then handle_request client else ()
    | InitReducer source ->
        let built = reducer_builder source in
        if (send_response client built) then handle_request client else ()
    | MapRequest (id, k, v) -> 
        let response = 
          if (mem activeWorkers id && find activeWorkers id = "mapper") then
            let results = Program.run id (k,v)
            in map_requester client id results
          else 
            send_response client (InvalidWorker(id)) 
        in if response then handle_request client else ()
    | ReduceRequest (id, k, v) -> 
        let response =  
          if (mem activeWorkers id && find activeWorkers id = "reducer") then
            let results = Program.run id (k,v) in 
            red_requester client id results
          else 
            send_response client (InvalidWorker(id))
        in if response then handle_request client else ())
  | None ->
      Connection.close client;
      print_endline "Connection lost while waiting for request."
