type ('a, 'b) t = 
  { capacity : int ref;
    size : int ref;    
    contents : ((('a * 'b) option) array) ref;
    hash : 'a -> int }

let create capacity hash = 
  { capacity = ref capacity;
    size = ref 0; 
    contents = ref (Array.make capacity None);
    hash = hash  }

let mem table key = 
  let i = (table.hash key) mod !(table.capacity) in 
  !(table.contents).(i) <> None

let iter f table =
  for i = 0 to !(table.capacity) - 1 do
    match !(table.contents).(i) with
    | None -> ()
    | Some (key, value) -> f key value
  done
    
let rec add table key value = 
  if !(table.capacity) <= !(table.size)*2 then resize table else ();
  if (not (mem table key)) then (table.size := !(table.size) + 1) else ();
  let i = (table.hash key) mod !(table.capacity) in
  !(table.contents).(i) <- Some (key, value)

and resize table =
  let table' = create (!(table.capacity)*2) table.hash in
  iter (fun k v -> add table' k v) table;
  table.contents := !(table'.contents);
  table.capacity := !(table'.capacity) 

let find table key = 
  let i = (table.hash key) mod !(table.capacity) in
  match !(table.contents).(i) with
  | None -> raise Not_found 
  | Some (key, value) -> value

let remove table key = 
  if (mem table key) then (table.size := !(table.size) - 1) else ();
  let i = (table.hash key) mod !(table.capacity) in 
  !(table.contents).(i) <- None
      
let fold f table init =  
  let f' acc kv_option =
    match kv_option with
    | None -> acc
    | Some (key, value) -> f key value acc
  in
  let acc = ref init in
  for i = 0 to !(table.capacity) - 1 do
    acc := f' !acc !(table.contents).(i);
  done;
  !acc

let length table = !(table.size)
