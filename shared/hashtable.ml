type ('a, 'b) t = 
  { capacity : int ref;
    size : int ref;    
    contents : ((('a * 'b) list) array) ref;
    hash : 'a -> int }

let create capacity hash = 
  { capacity = ref capacity;
    size = ref 0; 
    contents = ref (Array.make capacity []);
    hash = hash  }

let mem table key = 
  let i = (table.hash key) mod !(table.capacity) in
    List.fold_left (fun x (k,v) -> x || (k = key)) false !(table.contents).(i)

let iter f table =
  for i = 0 to !(table.capacity) - 1 do
    List.iter (fun (key, value) -> f key value) !(table.contents).(i)
  done

let remove table key = 
  if (mem table key) then 
    (table.size := !(table.size) - 1;
    let i = (table.hash key) mod !(table.capacity) in 
    let chain = !(table.contents).(i) in
    !(table.contents).(i) <- List.filter (fun (k,v) -> k <> key) chain)
  else ()
    
let rec add table key value = 
  if !(table.capacity) <= !(table.size)*2 then resize table else ();
  remove table key;  
  let i = (table.hash key) mod !(table.capacity) in
  !(table.contents).(i) <- (key, value)::(!(table.contents).(i));
  table.size := !(table.size) + 1

and resize table =
  let table' = create (!(table.capacity)*2) table.hash in
  iter (fun k v -> add table' k v) table;
  table.contents := !(table'.contents);
  table.capacity := !(table'.capacity)

let find table key =
  let i = (table.hash key) mod !(table.capacity) in
  let rec matcher chain =
    match chain with
    | [] -> raise Not_found
    | (k, v)::t -> if k = key then v else matcher t
  in matcher !(table.contents).(i)  

let fold f table init = 
  let f' acc (a, b) = f a b acc in
  let acc = ref init in
  for i = 0 to !(table.capacity) - 1 do
    acc := List.fold_left f' !acc !(table.contents).(i);
  done;
  !acc

let length table = !(table.size)
