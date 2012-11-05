open Util
open Worker_manager
(*
let map kv_pairs shared_data map_filename : (string * string) list = 
  let worker_manager =
    ref (Worker_manager.initialize_mappers map_filename shared_data)
  in
  let final_output = ref [] in
  let work_status = ref (Hashtbl.create 16) in
  List.iter (fun (k,v) -> Hashtbl.add !work_status (k,v) false) kv_pairs;
  let remaining_work = ref kv_pairs in
  let pool = ref (Thread_pool.create 20) in
  let m = Mutex.create () in
  let assign (k,v) () =
    let worker = Worker_manager.pop_worker !worker_manager in
    match (Worker_manager.map worker k v) with
    | None -> ()
    | Some lst ->
        Worker_manager.push_worker !worker_manager worker;
        Mutex.lock m;
        final_output := (List.rev_append lst !final_output);
        Hashtbl.replace !work_status (k,v) true;
        Mutex.unlock m in
  while !remaining_work <> [] do
    List.iter 
      (fun (k,v) -> 
        Thread.delay 0.1; 
        Thread_pool.add_work (assign (k,v)) !pool) !remaining_work;
    remaining_work := Hashtbl.fold 
      (fun kv is_done acc -> 
        if not is_done then kv::acc else acc) !work_status []
  done;
  Worker_manager.clean_up_workers !worker_manager;
  Thread_pool.destroy !pool;
  !final_output
  *)

type worker_types = Map | Reduce

(*
    match map_or_reduce with
    | Map -> ref (Worker_manager.initialize_mappers filename shared_data)
    | Reduce -> ref (Worker_manager.initialize_reducers filename shared_data) in
*)

(*
      match map_or_reduce with
      | Map -> Worker_manager.map worker k v
      | Reduce -> Worker_manager.reduce worker k v in
*)

(*
          (match map_or_reduce with 
          | Map -> List.rev_append lst !final_output
          | Reduce -> (k,lst)::(!final_output));
*)

let helper pairs worker_manager work output =
  let final_output = ref [] in
  let work_status = ref (Hashtbl.create 16) in
  List.iter (fun (k,v) -> Hashtbl.add !work_status (k,v) false) pairs;
  let remaining_work = ref pairs in
  let pool = ref (Thread_pool.create 20) in
  let m = Mutex.create () in
  let assign (k,v) () =
    let worker = Worker_manager.pop_worker !worker_manager in
    let results = work worker k v in
    match results with
    | None -> ()
    | Some lst ->
        Worker_manager.push_worker !worker_manager worker;
        Mutex.lock m;
        output k lst final_output;
        Hashtbl.replace !work_status (k,v) true;
        Mutex.unlock m in
  while !remaining_work <> [] do
    List.iter 
      (fun (k,v) -> 
        Thread.delay 0.1; 
        Thread_pool.add_work (assign (k,v)) !pool) !remaining_work;
    remaining_work := Hashtbl.fold 
      (fun kv is_done acc -> 
        if not is_done then kv::acc else acc) !work_status []
  done;
  Thread_pool.destroy !pool;
  Worker_manager.clean_up_workers !worker_manager;
  !final_output

let map kv_pairs shared_data map_filename : (string * string) list =
  let worker_manager = 
    ref (Worker_manager.initialize_mappers map_filename shared_data) in
  let work =
    Worker_manager.map in
  let output k lst final_output = 
    final_output := List.rev_append lst !final_output in
  helper kv_pairs worker_manager work output
(*
  let worker_manager =
    ref (Worker_manager.initialize_mappers map_filename shared_data)
  in
  let final_output = ref [] in
  let remaining_work = ref kv_pairs in
  let pool = ref (Thread_pool.create 20) in
  let m = Mutex.create () in
  while !remaining_work <> [] do
    let f () =
      match !remaining_work with
      | [] -> ()
      | (k,v)::t -> 
          let worker = Worker_manager.pop_worker !worker_manager in
          match (Worker_manager.map worker k v) with
          | None -> ()
          | Some lst ->
              Mutex.lock m;
              final_output := (List.rev_append lst !final_output);
              remaining_work := t;
              Mutex.unlock m;
              Worker_manager.push_worker !worker_manager worker 
    in Thread.delay 0.1; Thread_pool.add_work f !pool;
  done;
  Worker_manager.clean_up_workers !worker_manager;
  Thread_pool.destroy !pool;
  !final_output
*)

let combine kv_pairs : (string * string list) list = 
  let tbl = Hashtbl.create (List.length kv_pairs) in 
  let output = ref [] in
  let build_table () (k, v) = 
    if (Hashtbl.mem tbl k) then let vs = Hashtbl.find tbl k in
      Hashtbl.replace tbl k (v::vs)
    else
      Hashtbl.add tbl k [v]
  in List.fold_left build_table () kv_pairs;
  let make_list k vs = 
    output := (k, vs)::!output;
  in
  Hashtbl.iter make_list tbl; !output   

let reduce kvs_pairs shared_data reduce_filename : (string * string list) list =
  let worker_manager = 
    ref (Worker_manager.initialize_reducers reduce_filename "") in
  let work =
    Worker_manager.reduce in
  let output k lst final_output = 
    final_output := (k,lst)::(!final_output) in
  helper kvs_pairs worker_manager work output
(*
  let worker_manager = 
    ref (Worker_manager.initialize_reducers reduce_filename "")
  in
  let final_output = ref [] in
  let remaining_work = ref kvs_pairs in
  let pool = ref (Thread_pool.create 20) in
  let m = Mutex.create () in
  while !remaining_work <> [] do
    let f () =
      match !remaining_work with
      | [] -> ()
      | (k,vs)::t -> 
          let worker = Worker_manager.pop_worker !worker_manager in
          match (Worker_manager.reduce worker k vs) with
          | None -> ()
          | Some lst ->
              Mutex.lock m;
              final_output := ((k, lst)::!final_output);
              remaining_work := t;
              Mutex.unlock m;
              Worker_manager.push_worker !worker_manager worker 
    in Thread.delay 0.1; Thread_pool.add_work f !pool;
  done;
  Worker_manager.clean_up_workers !worker_manager;
  Thread_pool.destroy !pool;
  !final_output
*)
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


