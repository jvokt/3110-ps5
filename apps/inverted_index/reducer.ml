let (word, docs) = Program.get_input() in
let used = Hashtbl.create 16 in
let just_once acc doc = 
  if Hashtbl.mem used doc then acc
  else (Hashtbl.add used doc "present"; doc::acc) in 
let docs = List.fold_left just_once [] docs in
let c a b = 
  let a' = String.length a in
  let b' = String.length b in
  if a' < b' then -1 
  else if a' > b' then 1
  else compare a b in 
Program.set_output (List.sort c docs)
