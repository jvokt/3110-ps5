open Util

let print_page_ranks (pageranks : (int * float) list) : unit =
  List.iter (fun (k, v) -> Printf.printf ("Page: {'%d'} PageRank: {%f}\n") k v)
     (List.sort (fun (k1, _) (k2, _) -> compare k1 k2) pageranks)

let main (args : string array) : unit = 
  if Array.length args < 3 then
    Printf.printf "Usage: page_rank <num_iterations> <filename>"
  else 
    let filename = args.(3) in
    let iterations = args.(2) in
    let websites = load_websites filename in
    let add_website (w : website) : (string * string) =
      let id_s = string_of_int w.pageid in
      let links = marshal w.links in (id_s, links) in
    let id_links = List.map add_website websites in
    (* create a shared hashtable, initialize PageRank, iterate n times *)
    let n = float_of_int (List.length id_links) in
    let shared = Hashtbl.create 16 in
    List.iter (fun (id,links) -> Hashtbl.replace shared id (1.0/.n)) id_links;
    for i=1 to (int_of_string iterations) do
      let mapped = 
        Map_reduce.map id_links (marshal shared) "apps/page_rank/mapper.ml" in
      let combined = 
        Map_reduce.combine mapped in
      let reduced = 
        Map_reduce.reduce combined "" "apps/page_rank/reducer.ml" in
      List.iter
        (fun (id,pr) -> 
          match pr with
          | [pr] -> Hashtbl.replace shared id (float_of_string pr)
          | _ -> failwith "Reduce failed") reduced;
    done;
    let page_ranks = 
      Hashtbl.fold (fun a b c -> (int_of_string a,b)::c) shared [] in
    print_page_ranks page_ranks
