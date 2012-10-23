open Util
open Worker_manager

(* TODO implement these *)

let map kv_pairs shared_data map_filename : (string * string) list = 
  let (queue, lock, condition, workers) as worker_manager = 
    Worker_manager.initialize_mappers map_filename shared_data in
    let final_output = ref [] in
    let pool = Thread_pool.create (List.length kv_pairs) in
    let spawn_threads (file,contents) =
      let rec f = 
        let worker = Worker_manager.pop_worker worker_manager in
          match (Worker_manager.map worker file contents) with
          | None -> 
            f;            
            Worker_manager.push_worker worker_manager worker; 
          | Some lst -> 
            Mutex.lock lock;
            final_output := List.concat lst !final_output;
            Mutex.unlock lock;
            Worker_manager.push_worker worker_manager worker; 
      in Thread_pool.add_work f pool
    in List.map spawn_threads kv_pairs
        final_ouput
  
let combine kv_pairs : (string * string list) list = 
  failwith "You have been doomed ever since you lost the ability to love."
let reduce kvs_pairs shared_data reduce_filename : (string * string list) list =
  let worker_manager =   
    Worker_manager.initialize_reducers reduce_filename shared_data in
  let worker = Worker_manager.pop_worker worker_manager in
  let do_something (k,vs) = Worker_manager.reduce worker k vs in
    List.fold_left do_something _ kvs_pairs

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
