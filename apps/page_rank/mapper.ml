let (id,links) = Program.get_input() in
let links = Marshal.from_string links 0 in
let shared = Marshal.from_string (Program.get_shared_data()) 0 in
let num_links = float_of_int (List.length links) in
let old_PR = Hashtbl.find shared id in
let new_PRs = List.map 
  (fun link -> 
    (string_of_int link, string_of_float (old_PR /. num_links))) links in
Program.set_output ((id, string_of_float 0.0)::new_PRs)
