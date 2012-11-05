open Util
open Worker_manager

let helper pairs manager work output =
  let m = Mutex.create () in
  let final_output = ref [] in
  let status = ref (Hashtbl.create 16) in
  List.iter (fun kv -> Hashtbl.add !status kv false) pairs;
  let assign k v () =
    let worker = Worker_manager.pop_worker (!manager) in
    let results = work worker k v in
    match results with
    | None -> ()
    | Some lst -> 
       Worker_manager.push_worker (!manager) worker;
       Mutex.lock m;
       output k lst final_output;
       Hashtbl.replace (!status) (k,v) true;
       Mutex.unlock m in
  let pool = ref (Thread_pool.create 30) in
  let assign_work () = 
    let add_unfinished (k,v) is_done =
      if is_done then () 
      else Thread_pool.add_work (assign k v) !pool in
    Hashtbl.iter add_unfinished !status in
  let check () = 
    Hashtbl.fold (fun _ is_done acc -> acc && is_done) (!status) true in
  while not (check()) do
    assign_work();
    Thread.delay 0.1;
  done;
  Thread_pool.destroy (!pool); 
  clean_up_workers (!manager);
  !final_output

let map kv_pairs shared_data filename =
  let manager = 
    ref (Worker_manager.initialize_mappers filename shared_data) in
  let work =
     Worker_manager.map in
  let output k lst final_output =
    final_output := List.rev_append lst !final_output in
  helper kv_pairs manager work output

let combine kv_pairs : (string * string list) list = 
  let tbl = Hashtbl.create 16 in 
  let output = ref [] in
  let build_table () (k, v) = 
    if (Hashtbl.mem tbl k) then let vs = Hashtbl.find tbl k in
      Hashtbl.replace tbl k (v::vs)
    else
      Hashtbl.add tbl k [v]
  in List.fold_left build_table () kv_pairs;
  let make_list k vs = output := (k, vs)::!output in
  Hashtbl.iter make_list tbl; !output   

let reduce kvs_pairs shared_data filename = 
  let manager = 
    ref (Worker_manager.initialize_reducers filename shared_data) in
  let work =
     Worker_manager.reduce in
  let output k lst final_output =
    final_output := (k,lst)::(!final_output) in
  helper kvs_pairs manager work output

let map_reduce (app_name : string) (mapper : string) 
    (reducer : string) (filename : string) =
  let app_dir = Printf.sprintf "apps/%s/" app_name in
  let docs = load_documents filename in
  let titles = Hashtable.create 16 Hashtbl.hash in
  let add_document (d : document) : (string * string) =
    let id_s = string_of_int d.id in
    Hashtable.add titles id_s d.title; (id_s, d.body) in
  let kv_pairs = List.map add_document docs in
  let mapped = map kv_pairs "" (app_dir ^ mapper ^ ".ml") in
  let combined = combine mapped in
  let reduced = reduce combined  "" (app_dir ^ reducer ^ ".ml") in
  (titles, reduced)
