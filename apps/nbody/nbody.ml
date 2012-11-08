open Util

let product l1 l2 = 
  let rec product lst1 lst2 =
    match lst1,lst2 with
    | [],[] | [],_ -> []
    | h1::t1,[] -> product t1 l2
    | h1::t1,h2::t2 -> 
        if h1 <> h2 then (Util.marshal h1, Util.marshal h2)::(product lst1 t2) 
        else product lst1 t2 in
  product l1 l2

(* Create a transcript of body positions for `steps` time steps *)
let make_transcript (bodies : (string * body) list) (steps : int) : string = 
  let transcript = ref (string_of_bodies bodies) in
  let bodies = ref (List.fold_left (fun acc (_,body) -> body::acc) [] bodies) in
  for i=1 to steps do
    let body_tuples = product !bodies !bodies in
    let mapped = 
      Map_reduce.map body_tuples "" "apps/nbody/mapper.ml" in
    let combined = 
      Map_reduce.combine mapped in
    let reduced = 
      Map_reduce.reduce combined "" "apps/nbody/reducer.ml" in
    let unpack (acc1, acc2) (s, lst) =
      match lst with
      | [mar_body] -> 
          ((Util.unmarshal mar_body)::acc1, (s,Util.unmarshal mar_body)::acc2) 
      | _ -> failwith "malformed reduce results" in
    let (just_bodies, results) = List.fold_left unpack ([],[]) reduced in   
    bodies := just_bodies;
    transcript := !transcript ^ (string_of_bodies results);
  done;
  !transcript

let simulation_of_string = function
  | "binary_star" -> Simulations.binary_star
  | "diamond" -> Simulations.diamond
  | "orbit" -> Simulations.orbit
  | "swarm" -> Simulations.swarm
  | "system" -> Simulations.system
  | "terrible_situation" -> Simulations.terrible_situation
  | "zardoz" -> Simulations.zardoz
  | _ -> failwith "Invalid simulation name. Check `shared/simulations.ml`"

let main (args : string array) : unit = 
  if Array.length args < 3 then 
    print_endline "Usage: nbody <simulation> [<outfile>]
  <simulation> is the name of a simulation from shared/simulations.ml
  Results will be written to [<outfile>] or stdout."
  else begin
    let (num_bodies_str, bodies) = simulation_of_string args.(2) in
    let transcript = make_transcript bodies 60 in
    let out_channel = 
      if Array.length args > 3 then open_out args.(3) else stdout in
    output_string out_channel (num_bodies_str ^ "\n" ^ transcript);
    close_out out_channel end
