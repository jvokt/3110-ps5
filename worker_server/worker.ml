open Protocol

(* Hashtable helpers *)

(* Given a list of (key, value) pairs, return a corresponding hash table *)
let make_hashtbl (lst : ('a * 'b) list) : ('a, 'b) Hashtbl.t =
  let hashtbl = Hashtbl.create (List.length lst) in
  let _ = List.iter (fun (key, value) -> Hashtbl.add hashtbl key value) lst in
  hashtbl

let hashtbl_add hsh k v = Hashtbl.add hsh k v; hsh

let hashtbl_find = Hashtbl.find

let in_hashtbl = Hashtbl.mem

let activeWorkers = make_hashtbl lst in  

let send_response client response =
  let success = Connection.output client response in
    (if not success then
      (Connection.close client;
       print_endline "Connection lost before response could be sent.")
    else ());
    success

(* Additionally, the new mapper/reducer id must be added to the
   appropriate collection to track active mapper/reducers
   send_response client (Mapper (Program.build source))*)
(* Repatedly call itself. Use shared data somewhere. *)

let rec handle_request client =
  match Connection.input client with
    Some v ->
      begin
        match v with
        | InitMapper (source, shared_data) -> 
          let build = 
            (match Program.build source with 
            | (Some(id), "") -> 
              Mapper(Some(id),shared_data)
            | (None, error_msg) -> 
              Mapper(None, error_msg)) in
          if (send_response client build) then
            handle_request client
          else ()
        | InitReducer source -> 
          let build = 
            (match Program.build source with 
            | (Some(id), "") -> 
              Reducer(Some(id),shared_data)
            | (None, error_msg) -> 
              Reducer(None, error_msg)) in
          if (send_response client build) then
            handle_request client
          else ()
        | MapRequest (id, k, v) -> 
          if (* worker is valid *) then
            let results = Program.run id input in
            match results with
            | None -> send_response client (RuntimeError(id,"Error"))
            | Some result -> 
              let result' = 
                List.map (fun x -> marshall x) result in
              MapResults(id,result')
          else send_response client (InvalidWorker (id))
        | ReduceRequest (id, k, v) -> 
          if (* worker is valid *) then
            let results = Program.run id input in
            match results with
            | None -> send_response client (RuntimeError(id,"Error"))
            | Some result -> 
              let result' = 
                List.map (fun x -> marshall x) result in
              ReduceResults(id,result')
          else send_response client (InvalidWorker (id))
      end
  | None ->
      Connection.close client;
      print_endline "Connection lost while waiting for request."
