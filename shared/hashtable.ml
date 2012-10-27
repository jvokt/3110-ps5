type ('a, 'b) t = 
  { items : int ref;
    table : ((('a * 'b) list) array) ref;
    hash : 'a -> int }

let create capacity hash = 
  { items = ref 0; 
    table = ref (Array.make capacity []);
    hash = hash }
    
let rec resize h =
  let n = Array.length !(h.table) in
  let h' = create (2 * n) h.hash in
  for i = 0 to n - 1 do
    List.iter
      (fun (k,v) -> add h' k v) 
      !(h.table).(i)
  done;
  h.table := !(h'.table)
    
and add table key value = 
  if !(table.items) >= Array.length !(table.table) * 2 then 
    resize table;
  let i = table.hash key in 
  !(table.table).(i) <- (key, value)::!(table.table).(i);
  table.items := !(table.items) + 1

(* needs to raise Not_found exception appropriately *)  
let find table key = 
  let i = table.hash key in
  snd (List.hd !(table.table).(i))
(*  let i = table.hash key in
  try Some (List.assoc key !(table.table).(i)) 
  with Not_found -> None
*)

let mem table key = 
  let i = table.hash key in
  if !(table.table).(i) = [] then false else true

let remove table key = 
  let i = table.hash key in 
  let rec pop acc l = 
    match l with 
    | [] -> (false,List.rev acc)
    | (k',v)::t -> if k' = key then (true,List.rev_append acc t) else pop ((k',v)::acc) t in 
  let b,l' = pop [] !(table.table).(i) in 
  !(table.table).(i) <- l';
  if b then table.items := !(table.items) - 1 else ()

let iter (f : 'a -> 'b -> unit) table =
  let stop = (Array.length !(table.table)) - 1 in
  let f' (a, b) =
    f a b
  in
  for i = 0 to stop do
    List.hd (List.map f' !(table.table).(i));
  done
      
let fold f table init = 
  let stop = (Array.length !(table.table)) - 1 in
  let f' acc (a, b) =
    f a b acc
  in
  let acc = ref init in
  for i = 0 to stop do
    acc := List.fold_left f' !acc !(table.table).(i);
  done;
  !acc

let length table = table.items
