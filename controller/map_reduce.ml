open Util
open Worker_manager

(* TODO implement these *)

let map kv_pairs shared_data map_filename : (string * string) list = 
  let (* (queue, lock, condition, workers) *) worker_manager =
    Worker_manager.initialize_mappers map_filename shared_data (* as worker_manager *)
  in
    let final_output = ref [] in
    let pool = Thread_pool.create (List.length kv_pairs) in
    let append_lock = Mutex.create () in
    let spawn_threads () (file,contents) =
      let rec f () = 
        let worker = Worker_manager.pop_worker worker_manager in
          match (Worker_manager.map worker file contents) with
          | None -> f ()
            (*Worker_manager.push_worker worker_manager worker; *)
          | Some lst -> 
              Mutex.lock append_lock;
              final_output := (List.rev_append (List.rev lst) !final_output);
              Mutex.unlock append_lock;
              Worker_manager.push_worker worker_manager worker
      in Thread_pool.add_work f pool
    in List.fold_left spawn_threads () kv_pairs;
       Worker_manager.clean_up_workers worker_manager;
       Thread_pool.destroy pool;
        !final_output
  
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
    Worker_manager.initialize_reducers reduce_filename shared_data in
  let final_output = ref [] in
  let pool = Thread_pool.create (List.length kvs_pairs) in
  let append_lock = Mutex.create () in
    let spawn_threads () (key,values) =
        let rec f () = 
        let worker = Worker_manager.pop_worker worker_manager in
          match (Worker_manager.reduce worker key values) with
          | None -> f ()
          | Some lst -> 
              Mutex.lock append_lock;
              final_output := (key,lst):: (!final_output);
              Mutex.unlock append_lock;
              Worker_manager.push_worker worker_manager worker
      in Thread_pool.add_work f pool
    in List.fold_left spawn_threads () kvs_pairs;
       Worker_manager.clean_up_workers worker_manager;
       Thread_pool.destroy pool;
        !final_output

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
