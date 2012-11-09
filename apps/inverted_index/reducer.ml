let (word, docs) = Program.get_input() in
let c a b = 
  let a' = String.length a in
  let b' = String.length b in
  if a' < b' then -1 
  else if a' > b' then 1
  else compare a b in 
Program.set_output (List.sort c docs)
