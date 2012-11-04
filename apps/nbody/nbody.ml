open Util

let acceleration (m1,l1,v1) (m2,l2,v2) =
  let scale = (cBIG_G *. m2 /. ((Plane.s_dist l1 l2)**2.0)) in
  Plane.scale_point scale (Plane.unit_vector l1 l2)

let product l1 l2 = 
  let rec product lst1 lst2 =
    match (lst1,lst2) with
    | ([],[]) | ([],_) -> []
    | (h1::t1,[]) -> product t1 l2
    | (h1::t1,h2::t2) -> 
      if h1 <> h2 then (h1,h2)::(product lst1 t2) else product lst1 t2 in
  product l1 l2

(* Create a transcript of body positions for `steps` time steps *)
let make_transcript (bodies : (string * body) list) (steps : int) : string = 
  let shared = Hashtbl.create 16 in
    List.iter (fun (id,body) -> Hashtbl.add shared id body) bodies;
  let body_ids = List.map (fun (id,_) -> id) bodies in
  let tuple_bodies = product body_ids body_ids in
  let transcript = ref (string_of_bodies bodies) in
  print_endline "Begin iterations:";
  for i=1 to steps do
    let mapped = 
      Map_reduce.map tuple_bodies (marshal shared) "apps/nbody/mapper.ml" in
    let combined = 
      Map_reduce.combine mapped in
    let reduced = 
      Map_reduce.reduce combined "" "apps/nbody/reducer.ml" in
    (* update the positions of and velocities of every body by one t-step *)
    List.iter
      (fun (id,a) -> 
        match a with
        | [a] -> let a = unmarshal a in
          let (m,p0,v0) = Hashtbl.find shared id in
          let (p1,v1) = 
            let half_accel = Plane.scale_point (1./.2.) a in
            (Plane.v_plus p0 (Plane.v_plus v0 half_accel), Plane.v_plus v0 a) in
          Hashtbl.replace shared id (m,p1,v1)
        | _ -> failwith "Reduce failed") reduced;
    let new_bodies = Hashtbl.fold (fun k v acc -> (k,v)::acc) shared [] in
    transcript := !transcript ^ (string_of_bodies new_bodies);
    print_endline "end iteration";
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
